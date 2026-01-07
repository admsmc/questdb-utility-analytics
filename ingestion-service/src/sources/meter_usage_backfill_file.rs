use std::{path::PathBuf, time::SystemTime};

use futures::Stream;
use rust_client::domain::MeterUsage;
use tokio::{fs::File, io::{AsyncBufReadExt, BufReader}};
use async_stream::try_stream;

use crate::pipeline::{Envelope, PipelineError, Source};

/// A simple NDJSON backfill source for `MeterUsage`.
///
/// Each line in the file is expected to be a JSON object with the same shape
/// as the HTTP ingestion "incoming" payload (ts, meter_id, kwh, etc.).
pub struct MeterUsageBackfillFileSource {
    path: PathBuf,
}

#[derive(serde::Deserialize)]
struct BackfillMeterUsage {
    ts: time::OffsetDateTime,
    meter_id: String,
    premise_id: Option<String>,
    kwh: f64,
    kvarh: Option<f64>,
    kva_demand: Option<f64>,
    quality_flag: Option<String>,
    source_system: Option<String>,
}

impl From<BackfillMeterUsage> for MeterUsage {
    fn from(i: BackfillMeterUsage) -> Self {
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

impl MeterUsageBackfillFileSource {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self { path: path.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backfill_meter_usage_parses_from_struct() {
        use time::macros::datetime;

        let parsed = BackfillMeterUsage {
            ts: datetime!(2024-01-01 00:00:00 UTC),
            meter_id: "m-123".to_string(),
            premise_id: None,
            kwh: 1.23,
            kvarh: None,
            kva_demand: None,
            quality_flag: None,
            source_system: Some("scada".to_string()),
        };
        assert_eq!(parsed.meter_id, "m-123");
        assert_eq!(parsed.kwh, 1.23);

        let usage: MeterUsage = parsed.into();
        assert_eq!(usage.meter_id, "m-123");
        assert_eq!(usage.kwh, 1.23);
        assert!(usage.premise_id.is_none());
    }
}

#[async_trait::async_trait]
impl Source<MeterUsage> for MeterUsageBackfillFileSource {
    async fn stream(
        &self,
    ) -> std::pin::Pin<Box<dyn Stream<Item = Result<Envelope<MeterUsage>, PipelineError>> + Send>> {
        let path = self.path.clone();
        let s = try_stream! {
            let file = File::open(&path).await.map_err(|e| {
                PipelineError::Source(format!("failed to open backfill file: {e}"))
            })?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();

            while let Some(line) = lines.next_line().await.map_err(|e| {
                PipelineError::Source(format!("failed to read backfill line: {e}"))
            })? {
                let parsed: BackfillMeterUsage = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(e) => {
                        metrics::counter!("backfill_meter_usage_parse_errors_total").increment(1);
                        Err(PipelineError::Source(format!(
                            "failed to parse backfill json line: {e}"
                        )))?
                    }
                };
                let usage: MeterUsage = parsed.into();
                yield Envelope {
                    payload: usage,
                    received_at: SystemTime::now(),
                };
            }
        };

        Box::pin(s)
    }
}
