use std::time::Duration;

use futures::StreamExt;
use rust_client::domain::GenerationOutput;
use sqlx::{postgres::PgPool, Postgres, QueryBuilder};

use crate::pipeline::{Envelope, PipelineError, Sink};

pub struct QuestDbGenerationSink {
    pool: PgPool,
    batch_size: usize,
    max_retries: u32,
    retry_backoff: Duration,
}

impl QuestDbGenerationSink {
    pub fn new(pool: PgPool, batch_size: usize, max_retries: u32, retry_backoff: Duration) -> Self {
        Self {
            pool,
            batch_size,
            max_retries,
            retry_backoff,
        }
    }

    async fn flush_batch(&self, batch: &[Envelope<GenerationOutput>]) -> Result<(), PipelineError> {
        if batch.is_empty() {
            return Ok(());
        }

        let mut attempt: u32 = 0;
        loop {
            let res = self.insert_batch(batch).await;
            match res {
                Ok(()) => {
                    // Successful write: record metrics.
                    let counter = metrics::counter!("questdb_ingested_records_total");
                    counter.increment(batch.len() as u64);

                    if let Some(min_received) = batch.iter().map(|e| e.received_at).min() {
                        if let Ok(dur) = std::time::SystemTime::now().duration_since(min_received) {
                            let hist = metrics::histogram!("ingest_end_to_end_latency_seconds");
                            hist.record(dur.as_secs_f64());
                        }
                    }

                    return Ok(());
                }
                Err(e) if attempt < self.max_retries => {
                    attempt += 1;
                    let sleep_for = self.retry_backoff * attempt;
                    tracing::warn!(
                        error = %e,
                        attempt,
                        "questdb generation sink flush failed, retrying with backoff"
                    );
                    tokio::time::sleep(sleep_for).await;
                }
                Err(e) => {
                    tracing::error!(error = %e, "questdb generation sink flush failed, giving up");
                    metrics::counter!("questdb_generation_sink_errors_total").increment(1);
                    return Err(PipelineError::Sink(e.to_string()));
                }
            }
        }
    }

    async fn insert_batch(&self, batch: &[Envelope<GenerationOutput>]) -> Result<(), sqlx::Error> {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO generation_output (ts, plant_id, unit_id, mw, mvar, status, fuel_type) ",
        );

        builder.push("VALUES ");
        builder.push_values(batch, |mut b, env| {
            let g = &env.payload;
            b.push_bind(g.ts)
                .push_bind(&g.plant_id)
                .push_bind(&g.unit_id)
                .push_bind(g.mw)
                .push_bind(&g.mvar)
                .push_bind(&g.status)
                .push_bind(&g.fuel_type);
        });

        let query = builder.build();
        query.execute(&self.pool).await.map(|_| ())
    }
}

#[async_trait::async_trait]
impl Sink<GenerationOutput> for QuestDbGenerationSink {
    async fn run<S>(&self, mut input: S) -> Result<(), PipelineError>
    where
        S: futures::Stream<Item = Result<Envelope<GenerationOutput>, PipelineError>> + Send + Unpin + 'static,
    {
        let mut buffer: Vec<Envelope<GenerationOutput>> = Vec::with_capacity(self.batch_size);

        while let Some(item) = input.next().await {
            let env = match item {
                Ok(env) => env,
                Err(e) => {
                    tracing::error!(error = %e, "error in upstream pipeline for QuestDbGenerationSink");
                    continue;
                }
            };

            buffer.push(env);
            if buffer.len() >= self.batch_size {
                self.flush_batch(&buffer).await?;
                buffer.clear();
            }
        }

        if !buffer.is_empty() {
            self.flush_batch(&buffer).await?;
        }

        Ok(())
    }
}