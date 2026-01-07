use crate::pipeline::{Envelope, PipelineError, Transform};
use rust_client::domain::{GenerationOutput, MeterUsage};
use time::macros::datetime;

/// Pure validation of a `MeterUsage` record.
///
/// Rules:
/// - kWh must be non-negative.
/// - ts must be within a broad sanity window [2000-01-01, 2100-01-01].
pub fn validate_meter_usage(env: Envelope<MeterUsage>) -> Result<Envelope<MeterUsage>, PipelineError> {
    let m = &env.payload;

    if m.kwh < 0.0 {
        return Err(PipelineError::Transform("kwh must be non-negative".to_string()));
    }

    let min_ts = datetime!(2000-01-01 00:00:00 UTC);
    let max_ts = datetime!(2100-01-01 00:00:00 UTC);

    if m.ts < min_ts || m.ts > max_ts {
        return Err(PipelineError::Transform("timestamp out of allowed range".to_string()));
    }

    Ok(env)
}

/// Pure validation of a `GenerationOutput` record.
///
/// Rules:
/// - MW must be non-negative.
/// - ts must be within the same sanity window as meter usage.
pub fn validate_generation_output(
    env: Envelope<GenerationOutput>,
) -> Result<Envelope<GenerationOutput>, PipelineError> {
    let g = &env.payload;

    if g.mw < 0.0 {
        return Err(PipelineError::Transform("mw must be non-negative".to_string()));
    }

    let min_ts = datetime!(2000-01-01 00:00:00 UTC);
    let max_ts = datetime!(2100-01-01 00:00:00 UTC);

    if g.ts < min_ts || g.ts > max_ts {
        return Err(PipelineError::Transform("timestamp out of allowed range".to_string()));
    }

    Ok(env)
}

#[derive(Clone, Default)]
pub struct MeterUsageValidation;

#[async_trait::async_trait]
impl Transform<MeterUsage, MeterUsage> for MeterUsageValidation {
    async fn apply(
        &self,
        input: Envelope<MeterUsage>,
    ) -> Result<Envelope<MeterUsage>, PipelineError> {
        match validate_meter_usage(input) {
            Ok(env) => Ok(env),
            Err(e) => {
                metrics::counter!("validation_meter_usage_rejected_total").increment(1);
                Err(e)
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct GenerationOutputValidation;

#[async_trait::async_trait]
impl Transform<GenerationOutput, GenerationOutput> for GenerationOutputValidation {
    async fn apply(
        &self,
        input: Envelope<GenerationOutput>,
    ) -> Result<Envelope<GenerationOutput>, PipelineError> {
        match validate_generation_output(input) {
            Ok(env) => Ok(env),
            Err(e) => {
                metrics::counter!("validation_generation_output_rejected_total").increment(1);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn meter_usage_validation_accepts_valid_record() {
        let env = Envelope {
            payload: MeterUsage {
                ts: datetime!(2024-01-01 00:00:00 UTC),
                meter_id: "m-1".to_string(),
                premise_id: None,
                kwh: 1.0,
                kvarh: None,
                kva_demand: None,
                quality_flag: None,
                source_system: None,
            },
            received_at: std::time::SystemTime::now(),
        };

        let res = validate_meter_usage(env);
        assert!(res.is_ok());
    }

    #[test]
    fn meter_usage_validation_rejects_negative_kwh() {
        let env = Envelope {
            payload: MeterUsage {
                ts: datetime!(2024-01-01 00:00:00 UTC),
                meter_id: "m-1".to_string(),
                premise_id: None,
                kwh: -0.1,
                kvarh: None,
                kva_demand: None,
                quality_flag: None,
                source_system: None,
            },
            received_at: std::time::SystemTime::now(),
        };

        let res = validate_meter_usage(env);
        assert!(matches!(res, Err(PipelineError::Transform(_))));
    }

    #[test]
    fn meter_usage_validation_rejects_out_of_range_ts() {
        let env = Envelope {
            payload: MeterUsage {
                ts: datetime!(1800-01-01 00:00:00 UTC),
                meter_id: "m-1".to_string(),
                premise_id: None,
                kwh: 1.0,
                kvarh: None,
                kva_demand: None,
                quality_flag: None,
                source_system: None,
            },
            received_at: std::time::SystemTime::now(),
        };

        let res = validate_meter_usage(env);
        assert!(matches!(res, Err(PipelineError::Transform(_))));
    }
}
