use serde::Deserialize;
use std::fs;

fn default_ilp_tcp_addr() -> String {
    "127.0.0.1:9009".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuestDbConfig {
    /// QuestDB Postgres wire protocol URI (used for pgwire sinks and SQL-based jobs).
    pub uri: String,
    pub max_connections: u32,

    /// QuestDB ILP TCP address (used by ILP sinks).
    #[serde(default = "default_ilp_tcp_addr")]
    pub ilp_tcp_addr: String,
}

fn default_max_body_bytes() -> usize {
    10 * 1024 * 1024 // 10 MiB
}

fn default_max_request_records() -> usize {
    5_000
}

fn default_max_line_bytes() -> usize {
    1024 * 1024 // 1 MiB
}

#[derive(Debug, Clone, Deserialize)]
pub struct HttpSourceConfig {
    pub http_bind_addr: String,
    pub channel_capacity: usize,

    /// Optional bearer token for simple auth.
    ///
    /// If set, clients must send: `Authorization: Bearer <token>`.
    #[serde(default)]
    pub auth_bearer_token: Option<String>,

    /// Maximum request body size (bytes). This is enforced at the HTTP layer.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,

    /// Maximum number of records accepted per HTTP request.
    #[serde(default = "default_max_request_records")]
    pub max_request_records: usize,

    /// Maximum NDJSON line size (bytes). Guards against pathological single-line payloads.
    #[serde(default = "default_max_line_bytes")]
    pub max_line_bytes: usize,

    /// If true, NDJSON endpoints return 400 on the first malformed line.
    /// If false (default), malformed lines are skipped and counted.
    #[serde(default)]
    pub ndjson_strict: bool,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SinkKind {
    Ilp,
    Pgwire,
}

fn default_sink_kind() -> SinkKind {
    SinkKind::Ilp
}

fn default_sink_workers() -> usize {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub struct SinkConfig {
    /// Which sink implementation to use.
    #[serde(default = "default_sink_kind")]
    pub kind: SinkKind,

    /// Number of parallel sink workers.
    ///
    /// For ILP, this controls how many concurrent TCP connections are used.
    #[serde(default = "default_sink_workers")]
    pub workers: usize,

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
