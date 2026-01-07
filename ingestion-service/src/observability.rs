use tracing_subscriber::EnvFilter;

pub fn init_tracing() {
    let filter = EnvFilter::from_default_env()
        .add_directive("ingestion_service=info".parse().unwrap_or_else(|_| "info".parse().unwrap()));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}
