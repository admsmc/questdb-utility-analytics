# QuestDB Utility Time-Series Analytics

This repository provides a QuestDB-backed environment to retroactively analyse large volumes of
electric utility time-series data for both generation and supply.

## Structure

- `infra/` – infrastructure definitions (e.g. QuestDB via Docker Compose).
- `sql/` – database schema and reusable analysis queries.
  - `sql/schema/` – DDL for core time-series and reference tables.
  - `sql/analysis/` – saved analytical SQL snippets.
- `rust-client/` – Rust library providing a strongly typed core over QuestDB.
  - `domain/` – domain types for meter usage, generation output, etc.
  - `db/` – typed database access built on `sqlx` (Postgres wire to QuestDB).
- `src/utility_ts_analytics/` – Python package with functional-style helpers.
  - `ingest/` – bulk load and ingestion helpers.
  - `queries/` – pure functions that generate SQL for common analyses.
- `config/` – environment configuration (YAML, etc.).
- `tests/` – automated tests.
- `notebooks/` – exploratory analysis notebooks.
- `data-samples/` – small, anonymised sample datasets for experimentation.

## Architecture: Rust core, Python edge

Rust (`rust-client/`) acts as the strongly typed, functional core that understands the QuestDB
schema and provides safe ingestion and query APIs. Python (`src/utility_ts_analytics/`) sits at
the edges, orchestrating workflows, running notebooks, and calling into the Rust core (directly
or via a thin service) where stronger guarantees are needed.

## Quick start

1. Start QuestDB via Docker:

   ```bash
   docker compose -f infra/docker-compose.yml up -d
   ```

2. Apply the schema files (`sql/schema/*.sql`) using the QuestDB web console or psql.

3. Use the `utility_ts_analytics.ingest` and `utility_ts_analytics.queries` modules from Python
   to orchestrate bulk loads and run analyses.

## Next steps

- Add concrete ingestion scripts that map your actual CSV/Parquet exports into the schema.
- Extend the `queries` modules with utility-specific analyses (losses, feeder loading, TOU
  profiles, etc.).
- Wire this into your preferred orchestration or notebook environment.
