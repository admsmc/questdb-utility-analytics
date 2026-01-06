use time::OffsetDateTime;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct GenerationOutput {
    pub ts: OffsetDateTime,
    pub plant_id: String,
    pub unit_id: Option<String>,
    pub mw: f64,
    pub mvar: Option<f64>,
    pub status: Option<String>,
    pub fuel_type: Option<String>,
}
