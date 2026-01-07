use std::{net::SocketAddr, sync::Arc, time::SystemTime};

use axum::{extract::State, routing::post, Json, Router};
use futures::{Stream, StreamExt};
use rust_client::domain::MeterUsage;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::pipeline::{Envelope, PipelineError, Source};

#[derive(Clone)]
struct SharedSender {
    tx: mpsc::Sender<Envelope<MeterUsage>>,
}

#[derive(Clone)]
pub struct HttpJsonSource {
    receiver: Arc<tokio::sync::Mutex<Option<mpsc::Receiver<Envelope<MeterUsage>>>>>,
}

#[derive(serde::Deserialize)]
struct IncomingMeterUsage {
    ts: time::OffsetDateTime,
    meter_id: String,
    premise_id: Option<String>,
    kwh: f64,
    kvarh: Option<f64>,
    kva_demand: Option<f64>,
    quality_flag: Option<String>,
    source_system: Option<String>,
}

impl From<IncomingMeterUsage> for MeterUsage {
    fn from(i: IncomingMeterUsage) -> Self {
        MeterUsage {
            ts: i.ts,
            meter_id: i.meter_id,
            premise_id: i.premise_id,
            kwh: i.kwh,
            kvarh: i.kvarh,
            kva_demand: i.kva_demand,
            quality_flag: i.quality_flag,
            source_system: i.source_system,
        }
    }
}

impl HttpJsonSource {
    pub async fn new(bind_addr: &str, channel_capacity: usize) -> Result<Self, PipelineError> {
        let (tx, rx) = mpsc::channel(channel_capacity);
        let shared = SharedSender { tx };

        let app = Router::new()
            .route("/ingest/meter_usage", post(ingest_meter_usage))
            .with_state(shared.clone());

        let addr: SocketAddr = bind_addr
            .parse()
            .map_err(|e| PipelineError::Source(format!("invalid bind addr: {e}")))?;

        tokio::spawn(async move {
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    if let Err(e) = axum::serve(listener, app.into_make_service()).await {
                        tracing::error!(error = %e, "HTTP JSON source server error");
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to bind HTTP JSON source listener");
                }
            }
        });

        Ok(Self {
            receiver: Arc::new(tokio::sync::Mutex::new(Some(rx))),
        })
    }
}

#[async_trait::async_trait]
impl Source<MeterUsage> for HttpJsonSource {
    async fn stream(
        &self,
    ) -> std::pin::Pin<
        Box<dyn Stream<Item = Result<Envelope<MeterUsage>, PipelineError>> + Send>,
    > {
        let mut guard = self.receiver.lock().await;
        let rx = guard
            .take()
            .expect("HttpJsonSource stream already taken; only one consumer supported");

        let stream = ReceiverStream::new(rx).map(Ok);
        Box::pin(stream)
    }
}

async fn ingest_meter_usage(
    State(sender): State<SharedSender>,
    Json(payload): Json<Vec<IncomingMeterUsage>>,
) -> Result<(), axum::http::StatusCode> {
    metrics::counter!("http_ingest_requests_total").increment(1);

    for incoming in payload {
        let usage: MeterUsage = incoming.into();
        let env = Envelope {
            payload: usage,
            received_at: SystemTime::now(),
        };

        if let Err(_e) = sender.tx.send(env).await {
            // Channel closed; treat as server error
            metrics::counter!("http_ingest_failed_total").increment(1);
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    Ok(())
}
