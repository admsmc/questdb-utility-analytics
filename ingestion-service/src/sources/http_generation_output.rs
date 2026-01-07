use std::{net::SocketAddr, sync::Arc, time::SystemTime};

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, State},
    routing::post,
    Json, Router,
};
use futures::{Stream, StreamExt, TryStreamExt};
use rust_client::domain::GenerationOutput;
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;

use crate::pipeline::{Envelope, PipelineError, Source};

#[derive(Clone)]
struct SharedSender {
    tx: mpsc::Sender<Envelope<GenerationOutput>>,
    auth_bearer_token: Option<String>,
    max_request_records: usize,
    max_line_bytes: usize,
    ndjson_strict: bool,
}

#[derive(Clone)]
pub struct HttpGenerationOutputSource {
    receiver: Arc<tokio::sync::Mutex<Option<mpsc::Receiver<Envelope<GenerationOutput>>>>>,
}

#[derive(serde::Deserialize)]
struct IncomingGenerationOutput {
    ts: String,
    plant_id: String,
    unit_id: Option<String>,
    mw: f64,
    mvar: Option<f64>,
    status: Option<String>,
    fuel_type: Option<String>,
}

fn parse_ts(ts: &str) -> Result<time::OffsetDateTime, axum::http::StatusCode> {
    use axum::http::StatusCode;
    use time::format_description::well_known::Rfc3339;

    time::OffsetDateTime::parse(ts.trim(), &Rfc3339).map_err(|_e| StatusCode::BAD_REQUEST)
}

fn incoming_to_output(i: IncomingGenerationOutput) -> Result<GenerationOutput, axum::http::StatusCode> {
    Ok(GenerationOutput {
        ts: parse_ts(&i.ts)?,
        plant_id: i.plant_id,
        unit_id: i.unit_id,
        mw: i.mw,
        mvar: i.mvar,
        status: i.status,
        fuel_type: i.fuel_type,
    })
}

impl HttpGenerationOutputSource {
    pub async fn new(
        bind_addr: &str,
        channel_capacity: usize,
        auth_bearer_token: Option<String>,
        max_body_bytes: usize,
        max_request_records: usize,
        max_line_bytes: usize,
        ndjson_strict: bool,
    ) -> Result<Self, PipelineError> {
        let (tx, rx) = mpsc::channel(channel_capacity);
        let shared = SharedSender {
            tx,
            auth_bearer_token,
            max_request_records,
            max_line_bytes,
            ndjson_strict,
        };

        let app = Router::new()
            .route("/ingest/generation_output", post(ingest_generation_output))
            .route("/ingest/generation_output/ndjson", post(ingest_generation_output_ndjson))
            .with_state(shared.clone())
            .layer(DefaultBodyLimit::max(max_body_bytes));

        let addr: SocketAddr = bind_addr
            .parse()
            .map_err(|e| PipelineError::Source(format!("invalid bind addr: {e}")))?;

        // Fail-fast: if we can't bind, return an error to the caller.
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| PipelineError::Source(format!(
                "failed to bind generation_output HTTP source: {e}"
            )))?;

        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app.into_make_service()).await {
                tracing::error!(error = %e, "HTTP generation_output source server error");
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
    headers: axum::http::HeaderMap,
    Json(payload): Json<Vec<IncomingGenerationOutput>>,
) -> Result<(), axum::http::StatusCode> {
    use axum::http::StatusCode;

    metrics::counter!("http_generation_ingest_requests_total").increment(1);

    crate::sources::http_json::authorize(
        &headers,
        &sender.auth_bearer_token,
        "http_generation_ingest_unauthorized_total",
    )?;

    if payload.len() > sender.max_request_records {
        metrics::counter!("http_generation_ingest_rejected_too_large_total").increment(1);
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    for incoming in payload {
        let output: GenerationOutput = incoming_to_output(incoming)?;
        let env = Envelope {
            payload: output,
            received_at: SystemTime::now(),
        };

        match sender.tx.try_send(env) {
            Ok(()) => {}
            Err(TrySendError::Full(_env)) => {
                metrics::counter!("http_generation_ingest_rejected_overloaded_total").increment(1);
                return Err(StatusCode::TOO_MANY_REQUESTS);
            }
            Err(TrySendError::Closed(_env)) => {
                metrics::counter!("http_generation_ingest_failed_total").increment(1);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ndjson_lenient_skips_bad_lines_and_accepts_good_lines() {
        let (tx, mut rx) = mpsc::channel(10);
        let sender = SharedSender {
            tx,
            auth_bearer_token: None,
            max_request_records: 10,
            max_line_bytes: 1024,
            ndjson_strict: false,
        };

        let body = Body::from(
            "{\"ts\":\"2024-01-01T00:00:00Z\",\"plant_id\":\"p\",\"mw\":1.0}\nnot json\n{\"ts\":\"2024-01-01T00:15:00Z\",\"plant_id\":\"p\",\"mw\":2.0}\n",
        );

        let headers = axum::http::HeaderMap::new();
        let res = ingest_generation_output_ndjson(State(sender), headers, body).await.unwrap();
        assert_eq!(res.0.accepted, 2);
        assert_eq!(res.0.parse_errors, 1);

        let mut seen = 0;
        while let Ok(_env) = rx.try_recv() {
            seen += 1;
        }
        assert_eq!(seen, 2);
    }
}

#[derive(Debug, serde::Serialize)]
struct IngestSummary {
    accepted: usize,
    parse_errors: usize,
}

async fn ingest_generation_output_ndjson(
    State(sender): State<SharedSender>,
    headers: axum::http::HeaderMap,
    body: Body,
) -> Result<axum::Json<IngestSummary>, axum::http::StatusCode> {
    use axum::http::StatusCode;

    metrics::counter!("http_generation_ingest_ndjson_requests_total").increment(1);

    crate::sources::http_json::authorize(
        &headers,
        &sender.auth_bearer_token,
        "http_generation_ingest_ndjson_unauthorized_total",
    )?;

    let reader = StreamReader::new(
        body.into_data_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
    );
    let mut lines = tokio::io::BufReader::new(reader).lines();

    let mut accepted: usize = 0;
    let mut parse_errors: usize = 0;

    while let Some(line) = lines
        .next_line()
        .await
        .map_err(|_e| StatusCode::BAD_REQUEST)?
    {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if line.len() > sender.max_line_bytes {
            metrics::counter!("http_generation_ingest_ndjson_rejected_line_too_large_total").increment(1);
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }

        if accepted + parse_errors + 1 > sender.max_request_records {
            metrics::counter!("http_generation_ingest_ndjson_rejected_too_large_total").increment(1);
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }

        let incoming: IncomingGenerationOutput = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_e) => {
                parse_errors += 1;
                metrics::counter!("http_generation_ingest_ndjson_parse_errors_total").increment(1);

                if sender.ndjson_strict {
                    return Err(StatusCode::BAD_REQUEST);
                }

                continue;
            }
        };

        let output: GenerationOutput = match incoming_to_output(incoming) {
            Ok(v) => v,
            Err(_e) => {
                parse_errors += 1;
                metrics::counter!("http_generation_ingest_ndjson_parse_errors_total").increment(1);

                if sender.ndjson_strict {
                    return Err(StatusCode::BAD_REQUEST);
                }

                continue;
            }
        };
        let env = Envelope {
            payload: output,
            received_at: SystemTime::now(),
        };

        match sender.tx.try_send(env) {
            Ok(()) => {
                accepted += 1;
            }
            Err(TrySendError::Full(_env)) => {
                metrics::counter!("http_generation_ingest_ndjson_rejected_overloaded_total").increment(1);
                return Err(StatusCode::TOO_MANY_REQUESTS);
            }
            Err(TrySendError::Closed(_env)) => {
                metrics::counter!("http_generation_ingest_failed_total").increment(1);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    Ok(axum::Json(IngestSummary {
        accepted,
        parse_errors,
    }))
}
