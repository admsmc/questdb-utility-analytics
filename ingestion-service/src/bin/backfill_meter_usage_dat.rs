use anyhow::{bail, Result};
use ingestion_service::{
    config::AppConfig,
    observability,
    pipeline::Pipeline,
    sinks::QuestDbSink,
    sources::MeterUsageDatFileSource,
    transform,
};
use rust_client::domain::MeterUsage;
use sqlx::postgres::PgPoolOptions;
use std::{env, sync::Arc, time::Duration};

/// Backfill `meter_usage` table from a pipe-delimited .dat file.
///
/// Usage:
///   backfill_meter_usage_dat <path_to_dat>
#[tokio::main]
async fn main() -> Result<()> {
    observability::init_tracing();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        bail!("usage: backfill_meter_usage_dat <dat_file_path>");
    }
    let file_path = &args[1];

    // Load configuration (INGESTION_CONFIG can point to a backfill-specific file).
    let cfg = AppConfig::load()?;

    // Create QuestDB pool
    let pool = PgPoolOptions::new()
        .max_connections(cfg.questdb.max_connections)
        .connect(&cfg.questdb.uri)
        .await?;

    let mu_cfg = &cfg.meter_usage;

    let sink = QuestDbSink::new(
        pool,
        mu_cfg.sink.batch_size,
        mu_cfg.sink.max_retries,
        Duration::from_millis(mu_cfg.sink.retry_backoff_ms),
    );

    let source = MeterUsageDatFileSource::new(file_path);

    let pipeline: Pipeline<_, MeterUsage, _> = Pipeline {
        source,
        transforms: vec![Arc::new(transform::MeterUsageValidation::default())],
        sink,
    };

    pipeline.run().await?;

    Ok(())
}