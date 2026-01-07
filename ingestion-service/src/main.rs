use anyhow::Result;
use ingestion_service::{
    config::{AppConfig, SinkKind},
    metrics_server,
    observability,
    pipeline::{Pipeline, Sink},
    sinks::{QuestDbGenerationSink, QuestDbIlpGenerationSink, QuestDbIlpMeterUsageSink, QuestDbSink},
    sources::{http_generation_output::HttpGenerationOutputSource, http_json::HttpJsonSource},
    transform,
};
use rust_client::domain::{GenerationOutput, MeterUsage};
use sqlx::postgres::PgPoolOptions;
use std::{net::SocketAddr, sync::Arc, time::Duration};

enum MeterUsageSink {
    Ilp(QuestDbIlpMeterUsageSink),
    Pgwire(QuestDbSink),
}

#[async_trait::async_trait]
impl Sink<MeterUsage> for MeterUsageSink {
    async fn run<S>(&self, input: S) -> Result<(), ingestion_service::pipeline::PipelineError>
    where
        S: futures::Stream<Item = Result<ingestion_service::pipeline::Envelope<MeterUsage>, ingestion_service::pipeline::PipelineError>>
            + Send
            + Unpin
            + 'static,
    {
        match self {
            Self::Ilp(s) => s.run(input).await,
            Self::Pgwire(s) => s.run(input).await,
        }
    }
}

enum GenerationSink {
    Ilp(QuestDbIlpGenerationSink),
    Pgwire(QuestDbGenerationSink),
}

#[async_trait::async_trait]
impl Sink<GenerationOutput> for GenerationSink {
    async fn run<S>(&self, input: S) -> Result<(), ingestion_service::pipeline::PipelineError>
    where
        S: futures::Stream<Item = Result<ingestion_service::pipeline::Envelope<GenerationOutput>, ingestion_service::pipeline::PipelineError>>
            + Send
            + Unpin
            + 'static,
    {
        match self {
            Self::Ilp(s) => s.run(input).await,
            Self::Pgwire(s) => s.run(input).await,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    observability::init_tracing();

    // Load configuration
    let cfg = AppConfig::load()?;

    // Start metrics server if configured
    if let Some(metrics_cfg) = &cfg.metrics {
        metrics_server::init(&metrics_cfg.bind_addr);
    }

    let mu_cfg = &cfg.meter_usage;
    let gen_cfg = &cfg.generation_output;

    let needs_pgwire = mu_cfg.sink.kind == SinkKind::Pgwire || gen_cfg.sink.kind == SinkKind::Pgwire;

    // Create QuestDB connection pool only if any pipeline uses pgwire.
    let pool = if needs_pgwire {
        Some(
            PgPoolOptions::new()
                .max_connections(cfg.questdb.max_connections)
                .connect(&cfg.questdb.uri)
                .await?,
        )
    } else {
        None
    };

    let ilp_addr: SocketAddr = cfg
        .questdb
        .ilp_tcp_addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid questdb.ilp_tcp_addr: {e}"))?;

    // Meter usage pipeline
    let mu_sink = match mu_cfg.sink.kind {
        SinkKind::Ilp => MeterUsageSink::Ilp(QuestDbIlpMeterUsageSink::new(
            ilp_addr,
            mu_cfg.sink.batch_size,
            mu_cfg.sink.max_retries,
            Duration::from_millis(mu_cfg.sink.retry_backoff_ms),
            mu_cfg.sink.workers,
        )),
        SinkKind::Pgwire => {
            let pool = pool.clone().expect("pgwire pool must be initialized");
            MeterUsageSink::Pgwire(QuestDbSink::new(
                pool,
                mu_cfg.sink.batch_size,
                mu_cfg.sink.max_retries,
                Duration::from_millis(mu_cfg.sink.retry_backoff_ms),
            ))
        }
    };
    let mu_source = HttpJsonSource::new(
        &mu_cfg.source.http_bind_addr,
        mu_cfg.source.channel_capacity,
        mu_cfg.source.auth_bearer_token.clone(),
        mu_cfg.source.max_body_bytes,
        mu_cfg.source.max_request_records,
        mu_cfg.source.max_line_bytes,
        mu_cfg.source.ndjson_strict,
    )
    .await?;
    let mu_pipeline: Pipeline<_, MeterUsage, _> = Pipeline {
        source: mu_source,
        transforms: vec![Arc::new(transform::MeterUsageValidation::default())],
        sink: mu_sink,
    };

    // Generation output pipeline
    let gen_sink = match gen_cfg.sink.kind {
        SinkKind::Ilp => GenerationSink::Ilp(QuestDbIlpGenerationSink::new(
            ilp_addr,
            gen_cfg.sink.batch_size,
            gen_cfg.sink.max_retries,
            Duration::from_millis(gen_cfg.sink.retry_backoff_ms),
            gen_cfg.sink.workers,
        )),
        SinkKind::Pgwire => {
            let pool = pool.expect("pgwire pool must be initialized");
            GenerationSink::Pgwire(QuestDbGenerationSink::new(
                pool,
                gen_cfg.sink.batch_size,
                gen_cfg.sink.max_retries,
                Duration::from_millis(gen_cfg.sink.retry_backoff_ms),
            ))
        }
    };
    let gen_source = HttpGenerationOutputSource::new(
        &gen_cfg.source.http_bind_addr,
        gen_cfg.source.channel_capacity,
        gen_cfg.source.auth_bearer_token.clone(),
        gen_cfg.source.max_body_bytes,
        gen_cfg.source.max_request_records,
        gen_cfg.source.max_line_bytes,
        gen_cfg.source.ndjson_strict,
    )
    .await?;
    let gen_pipeline: Pipeline<_, GenerationOutput, _> = Pipeline {
        source: gen_source,
        transforms: vec![Arc::new(transform::GenerationOutputValidation::default())],
        sink: gen_sink,
    };

    // Run both pipelines concurrently
    tokio::try_join!(mu_pipeline.run(), gen_pipeline.run())?;

    Ok(())
}
