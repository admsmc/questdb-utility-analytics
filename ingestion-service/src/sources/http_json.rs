use std::{net::SocketAddr, sync::Arc, time::SystemTime};

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, State},
    routing::post,
    Json, Router,
};
use futures::{Stream, StreamExt, TryStreamExt};
use rust_client::domain::MeterUsage;
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::StreamReader;

use crate::pipeline::{Envelope, PipelineError, Source};

#[derive(Clone)]
struct SharedSender {
    tx: mpsc::Sender<Envelope<MeterUsage>>,
    auth_bearer_token: Option<String>,
    max_request_records: usize,
    max_line_bytes: usize,
    ndjson_strict: bool,
}

#[derive(Clone)]
pub struct HttpJsonSource {
    receiver: Arc<tokio::sync::Mutex<Option<mpsc::Receiver<Envelope<MeterUsage>>>>>,
}

#[derive(serde::Deserialize)]
struct IncomingMeterUsage {
    ts: String,
    meter_id: String,
    premise_id: Option<String>,
    kwh: f64,
    kvarh: Option<f64>,
    kva_demand: Option<f64>,
    quality_flag: Option<String>,
    source_system: Option<String>,
}

fn parse_ts(ts: &str) -> Result<time::OffsetDateTime, axum::http::StatusCode> {
    use axum::http::StatusCode;
    use time::format_description::well_known::Rfc3339;

    time::OffsetDateTime::parse(ts.trim(), &Rfc3339).map_err(|_e| StatusCode::BAD_REQUEST)
}

fn incoming_to_usage(i: IncomingMeterUsage) -> Result<MeterUsage, axum::http::StatusCode> {
    Ok(MeterUsage {
        ts: parse_ts(&i.ts)?,
        meter_id: i.meter_id,
        premise_id: i.premise_id,
        kwh: i.kwh,
        kvarh: i.kvarh,
        kva_demand: i.kva_demand,
        quality_flag: i.quality_flag,
        source_system: i.source_system,
    })
}

impl HttpJsonSource {
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
            .route("/ingest/meter_usage", post(ingest_meter_usage))
            .route("/ingest/meter_usage/ndjson", post(ingest_meter_usage_ndjson))
            .with_state(shared.clone())
            .layer(DefaultBodyLimit::max(max_body_bytes));

        let addr: SocketAddr = bind_addr
            .parse()
            .map_err(|e| PipelineError::Source(format!("invalid bind addr: {e}")))?;

        // Fail-fast: if we can't bind, return an error to the caller.
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| PipelineError::Source(format!("failed to bind HTTP JSON source: {e}")))?;

        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app.into_make_service()).await {
                tracing::error!(error = %e, "HTTP JSON source server error");
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
    headers: axum::http::HeaderMap,
    Json(payload): Json<Vec<IncomingMeterUsage>>,
) -> Result<(), axum::http::StatusCode> {
    use axum::http::StatusCode;

    metrics::counter!("http_ingest_requests_total").increment(1);

    authorize(&headers, &sender.auth_bearer_token, "http_ingest_unauthorized_total")?;

    if payload.len() > sender.max_request_records {
        metrics::counter!("http_ingest_rejected_too_large_total").increment(1);
        return Err(StatusCode::PAYLOAD_TOO_LARGE);
    }

    for incoming in payload {
        let usage: MeterUsage = incoming_to_usage(incoming)?;
        let env = Envelope {
            payload: usage,
            received_at: SystemTime::now(),
        };

        match sender.tx.try_send(env) {
            Ok(()) => {}
            Err(TrySendError::Full(_env)) => {
                // Overloaded: apply load-shedding rather than holding the request open.
                metrics::counter!("http_ingest_rejected_overloaded_total").increment(1);
                return Err(StatusCode::TOO_MANY_REQUESTS);
            }
            Err(TrySendError::Closed(_env)) => {
                metrics::counter!("http_ingest_failed_total").increment(1);
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
            "{\"ts\":\"2024-01-01T00:00:00Z\",\"meter_id\":\"m-1\",\"kwh\":1.0}\nnot json\n{\"ts\":\"2024-01-01T00:15:00Z\",\"meter_id\":\"m-1\",\"kwh\":2.0}\n",
        );

        let headers = axum::http::HeaderMap::new();
        let res = ingest_meter_usage_ndjson(State(sender), headers, body).await.unwrap();
        assert_eq!(res.0.accepted, 2);
        assert_eq!(res.0.parse_errors, 1);

        // Drain accepted messages.
        let mut seen = 0;
        while let Ok(_env) = rx.try_recv() {
            seen += 1;
        }
        assert_eq!(seen, 2);
    }

    #[tokio::test]
    async fn auth_rejects_when_token_set() {
        let (tx, _rx) = mpsc::channel(10);
        let sender = SharedSender {
            tx,
            auth_bearer_token: Some("secret".to_string()),
            max_request_records: 10,
            max_line_bytes: 1024,
            ndjson_strict: false,
        };

        let headers = axum::http::HeaderMap::new();
        let body = Body::from("{}\n");
        let err = ingest_meter_usage_ndjson(State(sender), headers, body).await.unwrap_err();
        assert_eq!(err, axum::http::StatusCode::UNAUTHORIZED);
    }
}

#[derive(Debug, serde::Serialize)]
struct IngestSummary {
    accepted: usize,
    parse_errors: usize,
}

pub(crate) fn authorize(
    headers: &axum::http::HeaderMap,
    token: &Option<String>,
    metric_name: &'static str,
) -> Result<(), axum::http::StatusCode> {
    use axum::http::StatusCode;

    let Some(expected) = token else {
        return Ok(());
    };

    let Some(auth) = headers.get(axum::http::header::AUTHORIZATION) else {
        metrics::counter!(metric_name).increment(1);
        return Err(StatusCode::UNAUTHORIZED);
    };

    let Ok(auth) = auth.to_str() else {
        metrics::counter!(metric_name).increment(1);
        return Err(StatusCode::UNAUTHORIZED);
    };

    let Some(given) = auth.strip_prefix("Bearer ") else {
        metrics::counter!(metric_name).increment(1);
        return Err(StatusCode::UNAUTHORIZED);
    };

    if given != expected {
        metrics::counter!(metric_name).increment(1);
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(())
}

async fn ingest_meter_usage_ndjson(
    State(sender): State<SharedSender>,
    headers: axum::http::HeaderMap,
    body: Body,
) -> Result<axum::Json<IngestSummary>, axum::http::StatusCode> {
    use axum::http::StatusCode;

    metrics::counter!("http_ingest_ndjson_requests_total").increment(1);

    authorize(&headers, &sender.auth_bearer_token, "http_ingest_ndjson_unauthorized_total")?;

    // Convert Body -> data stream -> AsyncRead -> lines() for streaming NDJSON parsing.
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
            metrics::counter!("http_ingest_ndjson_rejected_line_too_large_total").increment(1);
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }

        if accepted + parse_errors + 1 > sender.max_request_records {
            metrics::counter!("http_ingest_ndjson_rejected_too_large_total").increment(1);
            return Err(StatusCode::PAYLOAD_TOO_LARGE);
        }

        let incoming: IncomingMeterUsage = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_e) => {
                parse_errors += 1;
                metrics::counter!("http_ingest_ndjson_parse_errors_total").increment(1);

                if sender.ndjson_strict {
                    return Err(StatusCode::BAD_REQUEST);
                }

                continue;
            }
        };

        let usage: MeterUsage = match incoming_to_usage(incoming) {
            Ok(v) => v,
            Err(_e) => {
                parse_errors += 1;
                metrics::counter!("http_ingest_ndjson_parse_errors_total").increment(1);

                if sender.ndjson_strict {
                    return Err(StatusCode::BAD_REQUEST);
                }

                continue;
            }
        };
        let env = Envelope {
            payload: usage,
            received_at: SystemTime::now(),
        };

        match sender.tx.try_send(env) {
            Ok(()) => {
                accepted += 1;
            }
            Err(TrySendError::Full(_env)) => {
                metrics::counter!("http_ingest_ndjson_rejected_overloaded_total").increment(1);
                return Err(StatusCode::TOO_MANY_REQUESTS);
            }
            Err(TrySendError::Closed(_env)) => {
                metrics::counter!("http_ingest_failed_total").increment(1);
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
        }
    }

    Ok(axum::Json(IngestSummary {
        accepted,
        parse_errors,
    }))
}
