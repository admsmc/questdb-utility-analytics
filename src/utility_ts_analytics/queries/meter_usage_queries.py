"""Helpers to build common QuestDB SQL for meter usage analysis.

These functions are deliberately pure: they just return SQL strings based on
parameters, leaving execution to the caller.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class TimeRange:
    start: str  # ISO-8601 timestamp string
    end: str


def load_profile_sql(meter_id: str, time_range: TimeRange) -> str:
    """SQL to fetch a time-ordered load profile for a single meter."""

    return f"""
SELECT ts, kwh
FROM meter_usage
WHERE meter_id = '{meter_id}'
  AND ts >= '{time_range.start}'
  AND ts <  '{time_range.end}'
ORDER BY ts;
""".strip()


def aggregated_segment_load_sql(
    segments: Iterable[str],
    time_range: TimeRange,
    sample_by: str = "1h",
) -> str:
    """SQL to aggregate kWh by customer segment over time."""

    segments_list = ", ".join(f"'{s}'" for s in segments)

    return f"""
SELECT
    mu.ts,
    c.segment,
    SUM(mu.kwh) AS total_kwh
FROM meter_usage mu
JOIN meters m ON mu.meter_id = m.meter_id
JOIN customers c ON m.customer_id = c.customer_id
WHERE mu.ts >= '{time_range.start}'
  AND mu.ts <  '{time_range.end}'
  AND c.segment IN ({segments_list})
SAMPLE BY {sample_by} ALIGN TO CALENDAR
GROUP BY segment, ts
ORDER BY ts, segment;
""".strip()
