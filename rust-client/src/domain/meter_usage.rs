use time::OffsetDateTime;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MeterUsage {
    pub ts: OffsetDateTime,
    pub meter_id: String,
    pub premise_id: Option<String>,
    pub kwh: f64,
    pub kvarh: Option<f64>,
    pub kva_demand: Option<f64>,
    pub quality_flag: Option<String>,
    pub source_system: Option<String>,
}
