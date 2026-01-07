use anyhow::Result;
use ingestion_service::{config::AppConfig, observability};
use sqlx::postgres::PgPoolOptions;

const LOSS_ALERT_THRESHOLD: f64 = 0.02; // > 2% triggers alert

#[tokio::main]
async fn main() -> Result<()> {
    observability::init_tracing();

    let cfg = AppConfig::load()?;

    let pool = PgPoolOptions::new()
        .max_connections(cfg.questdb.max_connections)
        .connect(&cfg.questdb.uri)
        .await?;

    // Schema is expected to be applied out-of-band via `sql/schema/*.sql`.
    // See `sql/schema/03_mapping_tables.sql` for the tables referenced below.

    // For now, recompute the entire feeder_energy_balance table from scratch.
    sqlx::query("TRUNCATE TABLE feeder_energy_balance;")
        .execute(&pool)
        .await?;

    // Insert feeder-level balance with alert flag when |loss_pct| > threshold.
    let insert_sql = r#"
        INSERT INTO feeder_energy_balance
        SELECT
            g.ts,
            g.feeder_id,
            g.feeder_kwh_gen,
            COALESCE(d.feeder_kwh_demand, 0)                                       AS feeder_kwh_demand,
            g.feeder_kwh_gen - COALESCE(d.feeder_kwh_demand, 0)                   AS loss_kwh,
            CASE WHEN g.feeder_kwh_gen = 0 THEN NULL
                 ELSE (g.feeder_kwh_gen - COALESCE(d.feeder_kwh_demand, 0)) / g.feeder_kwh_gen
            END                                                                   AS loss_pct,
            COALESCE(c.meter_coverage_pct, 1.0)                                   AS meter_coverage_pct,
            CASE
                WHEN c.meter_coverage_pct IS NULL THEN 1.0
                ELSE c.meter_coverage_pct
            END                                                                   AS data_quality_score,
            CASE
                WHEN g.feeder_kwh_gen = 0 THEN 'unknown'
                WHEN c.meter_coverage_pct IS NOT NULL AND c.meter_coverage_pct < 0.9 THEN 'data'
                WHEN t.topology_events > 0 THEN 'topology'
                WHEN th.theft_events > 0 AND (c.meter_coverage_pct IS NULL OR c.meter_coverage_pct >= 0.9) THEN 'theft'
                WHEN g.feeder_kwh_gen > 0
                     AND ABS((g.feeder_kwh_gen - COALESCE(d.feeder_kwh_demand, 0)) / g.feeder_kwh_gen) <= 0.05
                     THEN 'physics'
                ELSE 'unknown'
            END                                                                   AS cause_hint,
            CASE
                WHEN g.feeder_kwh_gen = 0 THEN FALSE
                WHEN ABS((g.feeder_kwh_gen - COALESCE(d.feeder_kwh_demand, 0)) / g.feeder_kwh_gen) > $1
                    THEN TRUE
                ELSE FALSE
            END                                                                   AS alert
        FROM (
            SELECT
                go.ts,
                pfm.feeder_id,
                SUM(go.mw) * 0.25 AS feeder_kwh_gen            -- assume 15-min intervals
            FROM generation_output go
            JOIN plant_feeder_map pfm
              ON pfm.plant_id = go.plant_id
             AND (pfm.unit_id IS NULL OR pfm.unit_id = go.unit_id)
             AND pfm.from_ts <= go.ts
             AND pfm.to_ts   >  go.ts
            GROUP BY go.ts, pfm.feeder_id
        ) g
        LEFT JOIN (
            SELECT
                mu.ts,
                mfm.feeder_id,
                SUM(mu.kwh * COALESCE(msm.kwh_multiplier, 1.0)) AS feeder_kwh_demand
            FROM meter_usage mu
            JOIN meter_feeder_map mfm
              ON mfm.meter_id = mu.meter_id
             AND mfm.from_ts <= mu.ts
             AND mfm.to_ts   >  mu.ts
            LEFT JOIN meter_scale_map msm
              ON msm.meter_id = mu.meter_id
             AND msm.from_ts <= mu.ts
             AND msm.to_ts   >  mu.ts
            GROUP BY mu.ts, mfm.feeder_id
        ) d
          ON d.ts = g.ts
         AND d.feeder_id = g.feeder_id
        LEFT JOIN (
            SELECT
                mfm.feeder_id,
                mu.ts,
                COUNT(DISTINCT mu.meter_id) * 1.0 / NULLIF(COUNT(DISTINCT mfm.meter_id), 0) AS meter_coverage_pct
            FROM meter_feeder_map mfm
            LEFT JOIN meter_usage mu
              ON mu.meter_id = mfm.meter_id
             AND mu.ts      >= mfm.from_ts
             AND mu.ts      <  mfm.to_ts
            GROUP BY mfm.feeder_id, mu.ts
        ) c
          ON c.ts = g.ts
         AND c.feeder_id = g.feeder_id
        LEFT JOIN (
            SELECT
                feeder_id,
                ts,
                COUNT(*) AS topology_events
            FROM topology_events
            GROUP BY feeder_id, ts
        ) t
          ON t.ts = g.ts
         AND t.feeder_id = g.feeder_id
        LEFT JOIN (
            SELECT
                mfm.feeder_id,
                me.ts,
                COUNT(*) AS theft_events
            FROM meter_events me
            JOIN meter_feeder_map mfm
              ON mfm.meter_id = me.meter_id
             AND mfm.from_ts <= me.ts
             AND mfm.to_ts   >  me.ts
            WHERE me.event_type IN ('tamper', 'reverse_run', 'magnetic', 'theft_suspect')
            GROUP BY mfm.feeder_id, me.ts
        ) th
          ON th.ts = g.ts
         AND th.feeder_id = g.feeder_id;
        "#;

    let result = sqlx::query(insert_sql)
        .bind(LOSS_ALERT_THRESHOLD)
        .execute(&pool)
        .await?;

    let inserted = result.rows_affected();
    tracing::info!(
        inserted_rows = inserted,
        loss_alert_threshold = LOSS_ALERT_THRESHOLD,
        "feeder_energy_balance recomputed"
    );

    Ok(())
}