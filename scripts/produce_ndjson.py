#!/usr/bin/env python3
"""Tiny NDJSON producer for ingestion-service.

This script prints NDJSON to stdout so you can pipe into curl:

  python3 scripts/produce_ndjson.py meter-usage --count 1000 \
    | curl -sS -X POST -H 'Content-Type: application/x-ndjson' --data-binary @- \
      http://localhost:7001/ingest/meter_usage/ndjson

It uses only the Python standard library.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator


@dataclass(frozen=True)
class TimeSpec:
    start: datetime
    step: timedelta


def _parse_rfc3339_z(s: str) -> datetime:
    # Accept forms like: 2024-01-01T00:00:00Z
    if not s.endswith("Z"):
        raise ValueError("timestamp must end with 'Z'")
    base = s.removesuffix("Z")
    dt = datetime.fromisoformat(base)
    return dt.replace(tzinfo=timezone.utc)


def _format_rfc3339_z(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    # Keep seconds precision; append Z.
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _times(spec: TimeSpec, count: int) -> Iterator[datetime]:
    t = spec.start
    for _ in range(count):
        yield t
        t = t + spec.step


def meter_usage_records(
    *,
    meter_id: str,
    count: int,
    times: TimeSpec,
    kwh_base: float,
    kwh_step: float,
    premise_id: str | None,
) -> Iterator[dict[str, object]]:
    for i, ts in enumerate(_times(times, count)):
        yield {
            "ts": _format_rfc3339_z(ts),
            "meter_id": meter_id,
            "premise_id": premise_id,
            "kwh": round(kwh_base + i * kwh_step, 6),
        }


def generation_output_records(
    *,
    plant_id: str,
    unit_id: str | None,
    count: int,
    times: TimeSpec,
    mw_base: float,
    mw_step: float,
) -> Iterator[dict[str, object]]:
    for i, ts in enumerate(_times(times, count)):
        payload: dict[str, object] = {
            "ts": _format_rfc3339_z(ts),
            "plant_id": plant_id,
            "mw": round(mw_base + i * mw_step, 6),
        }
        if unit_id is not None:
            payload["unit_id"] = unit_id
        yield payload


def _write_ndjson(records: Iterator[dict[str, object]]) -> None:
    # Print compact JSON lines.
    for rec in records:
        print(json.dumps(rec, separators=(",", ":")))


def main() -> int:
    p = argparse.ArgumentParser(description="Generate NDJSON for ingestion-service endpoints")
    sub = p.add_subparsers(dest="cmd", required=True)

    def add_time_args(sp: argparse.ArgumentParser) -> None:
        sp.add_argument(
            "--start",
            default="2024-01-01T00:00:00Z",
            help="RFC3339 timestamp (Z) for first record (default: %(default)s)",
        )
        sp.add_argument(
            "--step-seconds",
            type=int,
            default=900,
            help="Seconds between records (default: %(default)s = 15 minutes)",
        )

    mu = sub.add_parser("meter-usage", help="Generate meter_usage NDJSON")
    mu.add_argument("--meter-id", default="m-1")
    mu.add_argument("--premise-id", default=None)
    mu.add_argument("--count", type=int, default=10)
    mu.add_argument("--kwh-base", type=float, default=1.0)
    mu.add_argument("--kwh-step", type=float, default=0.0)
    add_time_args(mu)

    go = sub.add_parser("generation-output", help="Generate generation_output NDJSON")
    go.add_argument("--plant-id", default="plant-1")
    go.add_argument("--unit-id", default=None)
    go.add_argument("--count", type=int, default=10)
    go.add_argument("--mw-base", type=float, default=10.0)
    go.add_argument("--mw-step", type=float, default=0.0)
    add_time_args(go)

    args = p.parse_args()

    start = _parse_rfc3339_z(args.start)
    times = TimeSpec(start=start, step=timedelta(seconds=int(args.step_seconds)))

    if args.cmd == "meter-usage":
        records = meter_usage_records(
            meter_id=str(args.meter_id),
            premise_id=(str(args.premise_id) if args.premise_id is not None else None),
            count=int(args.count),
            times=times,
            kwh_base=float(args.kwh_base),
            kwh_step=float(args.kwh_step),
        )
        _write_ndjson(records)
        return 0

    if args.cmd == "generation-output":
        records = generation_output_records(
            plant_id=str(args.plant_id),
            unit_id=(str(args.unit_id) if args.unit_id is not None else None),
            count=int(args.count),
            times=times,
            mw_base=float(args.mw_base),
            mw_step=float(args.mw_step),
        )
        _write_ndjson(records)
        return 0

    raise AssertionError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
