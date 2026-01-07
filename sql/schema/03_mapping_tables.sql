-- Mapping / auxiliary tables for the electric utility QuestDB project
--
-- NOTE: These tables are referenced by analytics queries and/or by the
-- ingestion-service binaries (e.g. feeder_balance). Keep schema DDL in
-- sql/schema/*.sql so binaries don't have to create tables at runtime.

-- Meter -> feeder mapping over time
CREATE TABLE IF NOT EXISTS meter_feeder_map (
    meter_id   SYMBOL,
    feeder_id  SYMBOL,
    from_ts    TIMESTAMP,
    to_ts      TIMESTAMP
) TIMESTAMP(from_ts)
PARTITION BY YEAR;

-- Plant/unit -> feeder mapping over time
CREATE TABLE IF NOT EXISTS plant_feeder_map (
    plant_id   SYMBOL,
    unit_id    SYMBOL,
    feeder_id  SYMBOL,
    from_ts    TIMESTAMP,
    to_ts      TIMESTAMP
) TIMESTAMP(from_ts)
PARTITION BY YEAR;

-- Meter scaling map for CT/PT, billing multipliers, etc.
CREATE TABLE IF NOT EXISTS meter_scale_map (
    meter_id         SYMBOL,
    account_id       SYMBOL,
    from_ts          TIMESTAMP,
    to_ts            TIMESTAMP,
    kwh_multiplier   DOUBLE,
    kw_multiplier    DOUBLE,
    kvarh_multiplier DOUBLE
) TIMESTAMP(from_ts)
PARTITION BY YEAR;

-- Topology events affecting network configuration
CREATE TABLE IF NOT EXISTS topology_events (
    ts          TIMESTAMP,
    feeder_id   SYMBOL,
    event_type  SYMBOL,
    details     STRING
) TIMESTAMP(ts)
PARTITION BY DAY;

-- Meter events (tamper, reverse run, etc.)
CREATE TABLE IF NOT EXISTS meter_events (
    ts          TIMESTAMP,
    meter_id    SYMBOL,
    event_type  SYMBOL,
    details     STRING
) TIMESTAMP(ts)
PARTITION BY DAY;

-- Derived analytics table for feeder-level energy balance
CREATE TABLE IF NOT EXISTS feeder_energy_balance (
    ts                  TIMESTAMP,
    feeder_id           SYMBOL,
    feeder_kwh_gen      DOUBLE,
    feeder_kwh_demand   DOUBLE,
    loss_kwh            DOUBLE,
    loss_pct            DOUBLE,
    meter_coverage_pct  DOUBLE,
    data_quality_score  DOUBLE,
    cause_hint          SYMBOL,
    alert               BOOLEAN
) TIMESTAMP(ts)
PARTITION BY MONTH;
