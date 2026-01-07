#!/usr/bin/env bash
set -euo pipefail

# Tiny examples for the NDJSON ingestion endpoints.
#
# Usage:
#   ./scripts/post_ndjson_examples.sh
#
# Override endpoints (defaults match ingestion-config.example.toml):
#   METER_USAGE_URL=http://localhost:7001 \
#   GENERATION_URL=http://localhost:7002 \
#   ./scripts/post_ndjson_examples.sh
#
# For HTTPS via the local TLS proxy:
#   METER_USAGE_URL=https://localhost:7443 \
#   GENERATION_URL=https://localhost:7443 \
#   CURL_INSECURE=1 \
#   ./scripts/post_ndjson_examples.sh

METER_USAGE_URL=${METER_USAGE_URL:-http://localhost:7001}
GENERATION_URL=${GENERATION_URL:-http://localhost:7002}

CURL_ARGS=("-sS" "-X" "POST")
if [[ "${CURL_INSECURE:-}" != "" ]]; then
  CURL_ARGS+=("-k")
fi

echo "Posting meter_usage NDJSON -> ${METER_USAGE_URL}/ingest/meter_usage/ndjson" >&2
cat <<'NDJSON' | curl "${CURL_ARGS[@]}" \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @- \
  "${METER_USAGE_URL}/ingest/meter_usage/ndjson" > /dev/null
{"ts":"2024-01-01T00:00:00Z","meter_id":"m-1","premise_id":"p-1","kwh":1.25,"kvarh":0.10,"kva_demand":2.0,"quality_flag":"ok","source_system":"scada"}
{"ts":"2024-01-01T00:15:00Z","meter_id":"m-1","premise_id":"p-1","kwh":1.10}
NDJSON

echo "Posting generation_output NDJSON -> ${GENERATION_URL}/ingest/generation_output/ndjson" >&2
cat <<'NDJSON' | curl "${CURL_ARGS[@]}" \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @- \
  "${GENERATION_URL}/ingest/generation_output/ndjson" > /dev/null
{"ts":"2024-01-01T00:00:00Z","plant_id":"plant-1","unit_id":"u-1","mw":10.0,"mvar":1.5,"status":"online","fuel_type":"gas"}
{"ts":"2024-01-01T00:15:00Z","plant_id":"plant-1","mw":9.5}
NDJSON

echo "Done." >&2
