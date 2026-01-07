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

   Note: `infra/docker-compose.yml` uses a Docker **named volume** for QuestDB data (to avoid Docker Desktop filesystem quirks). To wipe all DB data, remove the volume.

2. Create an ingestion config:

   ```bash
   cp ingestion-config.example.toml ingestion-config.toml
   ```

   By default, `ingestion-service` loads `ingestion-config.toml` from the current working directory.
   You can override the path with `INGESTION_CONFIG`.

3. Apply the schema files (`sql/schema/*.sql`) using the QuestDB web console or psql.

   Make sure you include `sql/schema/03_mapping_tables.sql` (mapping/aux tables used by
   feeder balance and scaling-aware queries).

4. Use the `utility_ts_analytics.ingest` and `utility_ts_analytics.queries` modules from Python
   to orchestrate bulk loads and run analyses.

## Ingestion performance: ILP vs pgwire

For high-throughput ingestion into QuestDB, prefer **ILP (Influx Line Protocol)**.

- ILP uses `questdb.ilp_tcp_addr` (default port 9009) and is the **default sink kind**.
- `pgwire` uses Postgres wire protocol via `sqlx` and can be useful when you want to reuse SQL tooling or keep everything on one port.

To switch a pipeline to pgwire, set:

- `meter_usage.sink.kind = "pgwire"` and/or
- `generation_output.sink.kind = "pgwire"`

## HTTP ingestion payloads: prefer NDJSON

For best ingestion performance over HTTP (lower peak memory and streaming parsing), use the NDJSON endpoints:

- `POST /ingest/meter_usage/ndjson`
- `POST /ingest/generation_output/ndjson`

Each line should be a single JSON object matching the corresponding payload schema.

### Quick curl examples

Meter usage (NDJSON):

```bash
cat <<'NDJSON' | curl -sS -X POST \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @- \
  http://localhost:7001/ingest/meter_usage/ndjson
{"ts":"2024-01-01T00:00:00Z","meter_id":"m-1","kwh":1.25}
{"ts":"2024-01-01T00:15:00Z","meter_id":"m-1","kwh":1.10}
NDJSON
```

Generation output (NDJSON):

```bash
cat <<'NDJSON' | curl -sS -X POST \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @- \
  http://localhost:7002/ingest/generation_output/ndjson
{"ts":"2024-01-01T00:00:00Z","plant_id":"plant-1","mw":10.0}
{"ts":"2024-01-01T00:15:00Z","plant_id":"plant-1","mw":9.5}
NDJSON
```

### Tiny producer scripts

Ready-to-run curl-based script (defaults match `ingestion-config.example.toml`):

```bash
./scripts/post_ndjson_examples.sh
```

Override endpoints:

```bash
METER_USAGE_URL=http://localhost:7001 \
GENERATION_URL=http://localhost:7002 \
./scripts/post_ndjson_examples.sh
```

Python NDJSON generator (prints NDJSON to stdout):

```bash
python3 scripts/produce_ndjson.py meter-usage --count 1000 \
  | curl -sS -X POST -H 'Content-Type: application/x-ndjson' --data-binary @- \
    http://localhost:7001/ingest/meter_usage/ndjson
```

```bash
python3 scripts/produce_ndjson.py generation-output --count 1000 \
  | curl -sS -X POST -H 'Content-Type: application/x-ndjson' --data-binary @- \
    http://localhost:7002/ingest/generation_output/ndjson
```

The existing JSON-array endpoints remain available:

- `POST /ingest/meter_usage`
- `POST /ingest/generation_output`

## Dedup / idempotency (ingestion retries)

The ingestion pipelines are designed for **at-least-once delivery**.

- On transient failures, the sinks retry writes with backoff.
- For ILP over TCP, a network error can happen after a partial write; retries may duplicate some records.

To make deduplication cheap and deterministic, the ILP sink emits a computed `event_id` tag per record.
Add `event_id SYMBOL` to your tables (included in `sql/schema/01_core_timeseries.sql`).

Example dedup patterns:

- Deduplicate by selecting a single row per `event_id` (e.g. using QuestDB’s “latest-by” query patterns, or an equivalent compaction job), then build your downstream aggregates from the deduplicated result.

## HTTP auth (optional)

You can enable a simple bearer token on each ingestion endpoint by setting `auth_bearer_token` under the relevant `*.source` config.
Clients must then send `Authorization: Bearer <token>`.

TLS is typically terminated at an ingress/reverse proxy; keep these endpoints private unless you add TLS termination.

## HTTPS (TLS termination) via local reverse proxy (Caddy)

For local/dev HTTPS, you can run the provided Caddy reverse proxy which terminates TLS on `:7443` and forwards to the ingestion-service HTTP ports on the host.

1. Ensure `ingestion-service` is running on the host (default ports `7001` and `7002`).
2. Start the proxy:

   ```bash
   docker compose -f infra/docker-compose.tls-proxy.yml up -d
   ```

3. Use the same ingestion paths over HTTPS:

   ```bash
   cat <<'NDJSON' | curl -k -sS -X POST \
     -H 'Content-Type: application/x-ndjson' \
     --data-binary @- \
     https://localhost:7443/ingest/meter_usage/ndjson
   {"ts":"2024-01-01T00:00:00Z","meter_id":"m-1","kwh":1.25}
   NDJSON
   ```

Notes:
- Config lives at `infra/caddy/Caddyfile`.
- The proxy uses Caddy’s `tls internal` for a local certificate; use `curl -k` or trust Caddy’s local root CA.
- On Linux, the compose file maps `host.docker.internal` to the host gateway (Docker 20.10+). If your Docker setup doesn’t support this, update `infra/caddy/Caddyfile` upstreams accordingly.

## Next steps

- Add concrete ingestion scripts that map your actual CSV/Parquet exports into the schema.
- Extend the `queries` modules with utility-specific analyses (losses, feeder loading, TOU
  profiles, etc.).
- Wire this into your preferred orchestration or notebook environment.
