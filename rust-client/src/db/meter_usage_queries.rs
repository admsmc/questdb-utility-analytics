use anyhow::Result;
use sqlx::PgPool;
use time::OffsetDateTime;

use crate::domain::MeterUsage;

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct AggregatedSegmentLoad {
    pub ts: OffsetDateTime,
    pub segment: String,
    pub total_kwh: f64,
}

/// Fetch a time-ordered load profile for a single meter.
pub async fn load_profile(
    pool: &PgPool,
    meter_id: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> Result<Vec<MeterUsage>> {
    let rows = sqlx::query_as::<_, MeterUsage>(
        r#"
        SELECT
            ts,
            meter_id,
            premise_id,
            kwh,
            kvarh,
            kva_demand,
            quality_flag,
            source_system
        FROM meter_usage
        WHERE meter_id = $1
          AND ts >= $2
          AND ts <  $3
        ORDER BY ts
        "#,
    )
    .bind(meter_id)
    .bind(start)
    .bind(end)
    .fetch_all(pool)
    .await?;

    Ok(rows)
}

/// Aggregate kWh by customer segment over time.
pub async fn aggregated_segment_load(
    pool: &PgPool,
    segments: &[String],
    start: OffsetDateTime,
    end: OffsetDateTime,
    sample_by: &str,
) -> Result<Vec<AggregatedSegmentLoad>> {
    // Build a dynamic list for the IN clause. For a small number of segments this
    // is acceptable; for large sets you would typically join against a temp table.
    let mut sql = format!(
        r#"
        SELECT
            mu.ts,
            c.segment,
            SUM(mu.kwh) AS total_kwh
        FROM meter_usage mu
        JOIN meters m ON mu.meter_id = m.meter_id
        JOIN customers c ON m.customer_id = c.customer_id
        WHERE mu.ts >= $1
          AND mu.ts <  $2
          AND c.segment = ANY($3)
        GROUP BY mu.ts, c.segment
        ORDER BY mu.ts, c.segment
        "#
    );

    // Note: QuestDB's `SAMPLE BY` is powerful but not supported directly in sqlx's
    // typed query builder, so we keep this example to a plain GROUP BY. You can
    // add resampling at the SQL level or in a higher-level aggregation layer.

    let rows = sqlx::query_as::<_, AggregatedSegmentLoad>(&sql)
        .bind(start)
        .bind(end)
        .bind(segments)
        .fetch_all(pool)
        .await?;

    Ok(rows)
}
