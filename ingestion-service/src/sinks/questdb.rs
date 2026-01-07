use std::time::Duration;

use futures::StreamExt;
use rust_client::domain::MeterUsage;
use sqlx::{postgres::PgPool, Postgres, QueryBuilder};

use crate::pipeline::{Envelope, PipelineError, Sink};

pub struct QuestDbSink {
    pool: PgPool,
    batch_size: usize,
    max_retries: u32,
    retry_backoff: Duration,
}

impl QuestDbSink {
    pub fn new(pool: PgPool, batch_size: usize, max_retries: u32, retry_backoff: Duration) -> Self {
        Self {
            pool,
            batch_size,
            max_retries,
            retry_backoff,
        }
    }

    async fn flush_batch(&self, batch: &[Envelope<MeterUsage>]) -> Result<(), PipelineError> {
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

                    // Approximate end-to-end latency from earliest received_at to now.
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
                        "questdb sink flush failed, retrying with backoff"
                    );
                    tokio::time::sleep(sleep_for).await;
                }
                Err(e) => {
                    tracing::error!(error = %e, "questdb sink flush failed, giving up");
                    metrics::counter!("questdb_sink_errors_total").increment(1);
                    return Err(PipelineError::Sink(e.to_string()));
                }
            }
        }
    }

    async fn insert_batch(&self, batch: &[Envelope<MeterUsage>]) -> Result<(), sqlx::Error> {
        let mut builder = QueryBuilder::<Postgres>::new(
            "INSERT INTO meter_usage (ts, meter_id, premise_id, kwh, kvarh, kva_demand, quality_flag, source_system) ",
        );

        builder.push("VALUES ");
        builder.push_values(batch, |mut b, env| {
            let m = &env.payload;
            b.push_bind(m.ts)
                .push_bind(&m.meter_id)
                .push_bind(&m.premise_id)
                .push_bind(m.kwh)
                .push_bind(&m.kvarh)
                .push_bind(&m.kva_demand)
                .push_bind(&m.quality_flag)
                .push_bind(&m.source_system);
        });

        let query = builder.build();
        query.execute(&self.pool).await.map(|_| ())
    }
}

#[async_trait::async_trait]
impl Sink<MeterUsage> for QuestDbSink {
    async fn run<S>(&self, mut input: S) -> Result<(), PipelineError>
    where
        S: futures::Stream<Item = Result<Envelope<MeterUsage>, PipelineError>> + Send + Unpin + 'static,
    {
        let mut buffer: Vec<Envelope<MeterUsage>> = Vec::with_capacity(self.batch_size);

        while let Some(item) = input.next().await {
            let env = match item {
                Ok(env) => env,
                Err(e) => {
                    tracing::error!(error = %e, "error in upstream pipeline for QuestDbSink");
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
