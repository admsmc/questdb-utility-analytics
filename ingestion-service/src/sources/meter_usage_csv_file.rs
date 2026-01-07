use std::{fs::File, path::PathBuf, time::SystemTime};

use csv::StringRecord;
use futures::Stream;
use rust_client::domain::MeterUsage;
use time::OffsetDateTime;

use crate::pipeline::{Envelope, PipelineError, Source};

/// CSV backfill/source for `MeterUsage`.
///
/// Expected header columns (by name):
/// - ts (RFC3339 timestamp)
/// - meter_id
/// - premise_id (optional)
/// - kwh
/// - kvarh (optional)
/// - kva_demand (optional)
/// - quality_flag (optional)
/// - source_system (optional)
pub struct MeterUsageCsvFileSource {
    path: PathBuf,
}

impl MeterUsageCsvFileSource {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self { path: path.into() }
    }
}

fn parse_optional_f64(s: &str) -> Option<f64> {
    if s.trim().is_empty() {
        None
    } else {
        s.parse().ok()
    }
}

fn parse_optional_string(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn record_to_meter_usage(record: &StringRecord, headers: &csv::StringRecord) -> Result<MeterUsage, PipelineError> {
    let get = |name: &str| -> Result<&str, PipelineError> {
        headers
            .iter()
            .position(|h| h == name)
            .and_then(|idx| record.get(idx))
            .ok_or_else(|| PipelineError::Source(format!("missing column '{name}' in CSV record")))
    };

    let ts_str = get("ts")?;
    let ts = OffsetDateTime::parse(ts_str.trim(), &time::format_description::well_known::Rfc3339)
        .map_err(|e| PipelineError::Source(format!("invalid ts '{ts_str}': {e}")))?;

    let meter_id = get("meter_id")?.to_string();
    let premise_id = parse_optional_string(get("premise_id").unwrap_or(""));

    let kwh_str = get("kwh")?;
    let kwh: f64 = kwh_str
        .trim()
        .parse()
        .map_err(|e| PipelineError::Source(format!("invalid kwh '{kwh_str}': {e}")))?;

    let kvarh = get("kvarh").ok().and_then(parse_optional_f64);
    let kva_demand = get("kva_demand").ok().and_then(parse_optional_f64);
    let quality_flag = get("quality_flag").ok().map(parse_optional_string).unwrap_or(None);
    let source_system = get("source_system").ok().map(parse_optional_string).unwrap_or(None);

    Ok(MeterUsage {
        ts,
        meter_id,
        premise_id,
        kwh,
        kvarh,
        kva_demand,
        quality_flag,
        source_system,
    })
}

#[async_trait::async_trait]
impl Source<MeterUsage> for MeterUsageCsvFileSource {
    async fn stream(
        &self,
    ) -> std::pin::Pin<Box<dyn Stream<Item = Result<Envelope<MeterUsage>, PipelineError>> + Send>> {
        // This source uses a blocking CSV reader but is wrapped in a single async task.
        // For large files, you might want to move this onto a dedicated thread pool.
        let path = self.path.clone();
        let s = async_stream::try_stream! {
            let file = File::open(&path)
                .map_err(|e| PipelineError::Source(format!("failed to open CSV file: {e}")))?;
            let mut rdr = csv::Reader::from_reader(file);
            let headers = rdr
                .headers()
                .map_err(|e| PipelineError::Source(format!("failed to read CSV headers: {e}")))?
                .clone();

            for result in rdr.records() {
                let record = result.map_err(|e| PipelineError::Source(format!(
                    "failed to read CSV record: {e}"
                )))?;

                let usage = match record_to_meter_usage(&record, &headers) {
                    Ok(u) => u,
                    Err(e) => {
                        metrics::counter!("meter_usage_csv_parse_errors_total").increment(1);
                        Err(e)?
                    }
                };

                yield Envelope {
                    payload: usage,
                    received_at: SystemTime::now(),
                };
            }
        };

        Box::pin(s)
    }
}