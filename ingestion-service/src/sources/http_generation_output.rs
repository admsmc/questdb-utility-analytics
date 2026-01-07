use std::{net::SocketAddr, sync::Arc, time::SystemTime};

use axum::{extract::State, routing::post, Json, Router};
use futures::{Stream, StreamExt};
use rust_client::domain::GenerationOutput;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::pipeline::{Envelope, PipelineError, Source};

#[derive(Clone)]
struct SharedSender {
    tx: mpsc::Sender<Envelope<GenerationOutput>>,
}

#[derive(Clone)]
pub struct HttpGenerationOutputSource {
    receiver: Arc<tokio::sync::Mutex<Option<mpsc::Receiver<Envelope<GenerationOutput>>>>>,
}

#[derive(serde::Deserialize)]
struct IncomingGenerationOutput {
    ts: time::OffsetDateTime,
    plant_id: String,
    unit_id: Option<String>,
    mw: f64,
    mvar: Option<f64>,
    status: Option<String>,
    fuel_type: Option<String>,
}

impl From<IncomingGenerationOutput> for GenerationOutput {
    fn from(i: IncomingGenerationOutput) -> Self {
        GenerationOutput {
            ts: i.ts,
            plant_id: i.plant_id,
            unit_id: i.unit_id,
            mw: i.mw,
            mvar: i.mvar,
            status: i.status,
            fuel_type: i.fuel_type,
        }
    }
}

impl HttpGenerationOutputSource {
    pub async fn new(bind_addr: &str, channel_capacity: usize) -> Result<Self, PipelineError> {
        let (tx, rx) = mpsc::channel(channel_capacity);
        let shared = SharedSender { tx };

        let app = Router::new()
            .route("/ingest/generation_output", post(ingest_generation_output))
            .with_state(shared.clone());

        let addr: SocketAddr = bind_addr
            .parse()
            .map_err(|e| PipelineError::Source(format!("invalid bind addr: {e}")))?;

        tokio::spawn(async move {
            match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => {
                    if let Err(e) = axum::serve(listener, app.into_make_service()).await {
                        tracing::error!(error = %e, "HTTP generation_output source server error");
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to bind generation_output listener");
                }
            }
        });

        Ok(Self {
            receiver: Arc::new(tokio::sync::Mutex::new(Some(rx))),
        })
    }
}

#[async_trait::async_trait]
impl Source<GenerationOutput> for HttpGenerationOutputSource {
    async fn stream(
        &self,
    ) -> std::pin::Pin<
        Box<dyn Stream<Item = Result<Envelope<GenerationOutput>, PipelineError>> + Send>,
    > {
        let mut guard = self.receiver.lock().await;
        let rx = guard
            .take()
            .expect("HttpGenerationOutputSource stream already taken; only one consumer supported");

        let stream = ReceiverStream::new(rx).map(Ok);
        Box::pin(stream)
    }
}

async fn ingest_generation_output(
    State(sender): State<SharedSender>,
    Json(payload): Json<Vec<IncomingGenerationOutput>>,
) -> Result<(), axum::http::StatusCode> {
    metrics::counter!("http_generation_ingest_requests_total").increment(1);

    for incoming in payload {
        let output: GenerationOutput = incoming.into();
        let env = Envelope {
            payload: output,
            received_at: SystemTime::now(),
        };

        if let Err(_e) = sender.tx.send(env).await {
            // Channel closed; treat as server error
            metrics::counter!("http_generation_ingest_failed_total").increment(1);
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    Ok(())
}
