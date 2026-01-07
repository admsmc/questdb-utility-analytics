from __future__ import annotations

from utility_ts_analytics.queries.meter_usage_queries import (
    TimeRange,
    aggregated_segment_load_sql,
    load_profile_sql,
)


def test_load_profile_sql_includes_scale_join_and_time_bounds() -> None:
    tr = TimeRange(start="2024-01-01T00:00:00Z", end="2024-01-02T00:00:00Z")
    sql = load_profile_sql(meter_id="m-1", time_range=tr)

    assert "FROM meter_usage mu" in sql
    assert "LEFT JOIN meter_scale_map" in sql
    assert "mu.meter_id = 'm-1'" in sql
    assert "mu.ts >= '2024-01-01T00:00:00Z'" in sql
    assert "mu.ts <  '2024-01-02T00:00:00Z'" in sql


def test_aggregated_segment_load_sql_includes_sample_by_and_segments() -> None:
    tr = TimeRange(start="2024-01-01T00:00:00Z", end="2024-01-02T00:00:00Z")
    sql = aggregated_segment_load_sql(segments=["res", "c&i"], time_range=tr, sample_by="1h")

    assert "SAMPLE BY 1h" in sql
    assert "c.segment IN ('res', 'c&i')" in sql
    assert "GROUP BY segment, ts" in sql
