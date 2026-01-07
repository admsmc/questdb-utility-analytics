use std::{fs::File, path::PathBuf, time::SystemTime};

use csv::StringRecord;
use futures::Stream;
use rust_client::domain::MeterUsage;
use time::OffsetDateTime;

use crate::pipeline::{Envelope, PipelineError, Source};

/// Pipe-delimited (`.dat`) source for `MeterUsage`.
///
/// Assumes a header row with the same column names as the CSV source, but
/// fields are separated by `|` instead of `,`.
pub struct MeterUsageDatFileSource {
    path: PathBuf,
}

impl MeterUsageDatFileSource {
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
            .ok_or_else(|| PipelineError::Source(format!("missing column '{name}' in DAT record")))
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
impl Source<MeterUsage> for MeterUsageDatFileSource {
    async fn stream(
        &self,
    ) -> std::pin::Pin<Box<dyn Stream<Item = Result<Envelope<MeterUsage>, PipelineError>> + Send>> {
        let path = self.path.clone();
        let s = async_stream::try_stream! {
            let file = File::open(&path)
                .map_err(|e| PipelineError::Source(format!("failed to open DAT file: {e}")))?;
            let mut rdr = csv::ReaderBuilder::new()
                .delimiter(b'|')
                .from_reader(file);
            let headers = rdr
                .headers()
                .map_err(|e| PipelineError::Source(format!("failed to read DAT headers: {e}")))?
                .clone();

            for result in rdr.records() {
                let record = result.map_err(|e| PipelineError::Source(format!(
                    "failed to read DAT record: {e}"
                )))?;

                let usage = match record_to_meter_usage(&record, &headers) {
                    Ok(u) => u,
                    Err(e) => {
                        metrics::counter!("meter_usage_dat_parse_errors_total").increment(1);
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