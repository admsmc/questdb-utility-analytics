from __future__ import annotations

from pathlib import Path

from utility_ts_analytics.ingest.bulk_loader import CopyJob, build_copy_sql, map_jobs


def test_build_copy_sql_includes_header_and_path() -> None:
    job = CopyJob(table="meter_usage", csv_path=Path("/var/lib/questdb/import/meter_usage.csv"))

    sql = build_copy_sql(job)

    assert sql.startswith("COPY meter_usage FROM '")
    assert "header true" in sql
    assert "/var/lib/questdb/import/meter_usage.csv" in sql
    assert sql.endswith(";")


def test_build_copy_sql_can_merge_extra_options() -> None:
    job = CopyJob(
        table="meter_usage",
        csv_path=Path("/var/lib/questdb/import/meter_usage.csv"),
        extra_options={"timestamp": "ts"},
    )

    sql = build_copy_sql(job)

    assert "timestamp ts" in sql


def test_map_jobs_is_pure_and_composable() -> None:
    jobs = [
        CopyJob(table="meter_usage", csv_path=Path("/a.csv")),
        CopyJob(table="generation_output", csv_path=Path("/b.csv")),
    ]

    def with_no_header(j: CopyJob) -> CopyJob:
        return CopyJob(table=j.table, csv_path=j.csv_path, has_header=False, extra_options=j.extra_options)

    out = map_jobs(with_no_header, jobs)

    assert [j.has_header for j in out] == [False, False]
    # original unchanged
    assert [j.has_header for j in jobs] == [True, True]
