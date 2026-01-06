-- Core time-series tables for the electric utility QuestDB project

CREATE TABLE IF NOT EXISTS meter_usage (
    ts              TIMESTAMP,
    meter_id        SYMBOL,
    premise_id      SYMBOL,
    kwh             DOUBLE,
    kvarh           DOUBLE,
    kva_demand      DOUBLE,
    quality_flag    SYMBOL,
    source_system   SYMBOL
) TIMESTAMP(ts)
PARTITION BY DAY;

CREATE TABLE IF NOT EXISTS generation_output (
    ts              TIMESTAMP,
    plant_id        SYMBOL,
    unit_id         SYMBOL,
    mw              DOUBLE,
    mvar            DOUBLE,
    status          SYMBOL,
    fuel_type       SYMBOL
) TIMESTAMP(ts)
PARTITION BY DAY;

CREATE TABLE IF NOT EXISTS network_measurements (
    ts              TIMESTAMP,
    feeder_id       SYMBOL,
    substation_id   SYMBOL,
    phase           SYMBOL,
    mw              DOUBLE,
    mvar            DOUBLE,
    kv              DOUBLE,
    current_a       DOUBLE
) TIMESTAMP(ts)
PARTITION BY DAY;
