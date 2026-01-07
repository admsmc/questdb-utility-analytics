use std::net::SocketAddr;

use axum::{routing::get, Router};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use once_cell::sync::OnceCell;

static PROM_HANDLE: OnceCell<PrometheusHandle> = OnceCell::new();

pub fn init(bind_addr: &str) {
    let builder = PrometheusBuilder::new();
    let handle = builder
        .install_recorder()
        .expect("failed to install Prometheus metrics recorder");

    // Ignore error if the handle was already set; this should only be called once.
    let _ = PROM_HANDLE.set(handle);

    let addr: SocketAddr = bind_addr
        .parse()
        .expect("invalid metrics bind address");

    tokio::spawn(async move {
        let app = Router::new().route("/metrics", get(metrics_handler));

        match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                if let Err(e) = axum::serve(listener, app.into_make_service()).await {
                    tracing::error!(error = %e, "metrics server error");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to bind metrics listener");
            }
        }
    });
}

async fn metrics_handler() -> String {
    PROM_HANDLE
        .get()
        .expect("Prometheus recorder not initialized")
        .render()
}
