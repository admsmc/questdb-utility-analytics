use anyhow::Result;
use ingestion_service::{
    config::AppConfig,
    metrics_server,
    observability,
    pipeline::Pipeline,
    sinks::{QuestDbGenerationSink, QuestDbSink},
    sources::{http_generation_output::HttpGenerationOutputSource, http_json::HttpJsonSource},
    transform,
};
use rust_client::domain::{GenerationOutput, MeterUsage};
use sqlx::postgres::PgPoolOptions;
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<()> {
    observability::init_tracing();

    // Load configuration
    let cfg = AppConfig::load()?;

    // Start metrics server if configured
    if let Some(metrics_cfg) = &cfg.metrics {
        metrics_server::init(&metrics_cfg.bind_addr);
    }

    // Create QuestDB connection pool
    let pool = PgPoolOptions::new()
        .max_connections(cfg.questdb.max_connections)
        .connect(&cfg.questdb.uri)
        .await?;

    // Meter usage pipeline
    let mu_cfg = &cfg.meter_usage;
    let mu_sink = QuestDbSink::new(
        pool.clone(),
        mu_cfg.sink.batch_size,
        mu_cfg.sink.max_retries,
        Duration::from_millis(mu_cfg.sink.retry_backoff_ms),
    );
    let mu_source = HttpJsonSource::new(&mu_cfg.source.http_bind_addr, mu_cfg.source.channel_capacity).await?;
    let mu_pipeline: Pipeline<_, MeterUsage, _> = Pipeline {
        source: mu_source,
        transforms: vec![Arc::new(transform::MeterUsageValidation::default())],
        sink: mu_sink,
    };

    // Generation output pipeline
    let gen_cfg = &cfg.generation_output;
    let gen_sink = QuestDbGenerationSink::new(
        pool,
        gen_cfg.sink.batch_size,
        gen_cfg.sink.max_retries,
        Duration::from_millis(gen_cfg.sink.retry_backoff_ms),
    );
    let gen_source = HttpGenerationOutputSource::new(
        &gen_cfg.source.http_bind_addr,
        gen_cfg.source.channel_capacity,
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
