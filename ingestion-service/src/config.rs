use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct QuestDbConfig {
    pub uri: String,
    pub max_connections: u32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpSourceConfig {
    pub http_bind_addr: String,
    pub channel_capacity: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinkConfig {
    pub batch_size: usize,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    pub name: String,
    pub source: HttpSourceConfig,
    pub sink: SinkConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    pub bind_addr: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub questdb: QuestDbConfig,
    pub meter_usage: PipelineConfig,
    pub generation_output: PipelineConfig,
    pub metrics: Option<MetricsConfig>,
}

impl AppConfig {
    pub fn load() -> anyhow::Result<Self> {
        use std::env;

        let path = env::var("INGESTION_CONFIG").unwrap_or_else(|_| "ingestion-config.toml".to_string());
        let contents = fs::read_to_string(&path)?;
        let cfg: AppConfig = toml::from_str(&contents)?;
        Ok(cfg)
    }
}
