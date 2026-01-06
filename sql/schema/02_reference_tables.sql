-- Reference / dimension tables for the electric utility QuestDB project

CREATE TABLE IF NOT EXISTS meters (
    meter_id        SYMBOL INDEX,
    premise_id      SYMBOL,
    customer_id     SYMBOL,
    feeder_id       SYMBOL,
    substation_id   SYMBOL,
    tariff_code     SYMBOL,
    install_date    DATE,
    retire_date     DATE,
    meter_type      SYMBOL
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id     SYMBOL INDEX,
    segment         SYMBOL,
    name            STRING,
    region_id       SYMBOL,
    lat             DOUBLE,
    lon             DOUBLE
);

CREATE TABLE IF NOT EXISTS plants (
    plant_id        SYMBOL INDEX,
    name            STRING,
    region_id       SYMBOL,
    fuel_type       SYMBOL,
    capacity_mw     DOUBLE
);
