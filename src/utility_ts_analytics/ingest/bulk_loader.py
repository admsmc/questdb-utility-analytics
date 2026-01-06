"""Bulk loading utilities for QuestDB.

The functions in this module are side-effecting at the boundary only (I/O),
with configuration and transformation expressed as pure functions where
possible.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Mapping


@dataclass(frozen=True)
class CopyJob:
    """Description of a QuestDB COPY job from a CSV file.

    This is intentionally declarative and immutable so that you can compose
    and test job definitions without touching QuestDB.
    """

    table: str
    csv_path: Path
    has_header: bool = True
    extra_options: Mapping[str, str] | None = None


def build_copy_sql(job: CopyJob) -> str:
    """Build the SQL COPY statement for a given job.

    This function is pure and easy to test.
    """

    options: dict[str, str] = {"header": "true" if job.has_header else "false"}
    if job.extra_options:
        options.update(job.extra_options)

    opts_sql = ", ".join(f"{k} {v}" for k, v in options.items())

    # QuestDB requires a server-side path.
    return (
        f"COPY {job.table} FROM '{job.csv_path}' "
        + (f"WITH {opts_sql}" if opts_sql else "")
        + ";"
    )


def map_jobs(transform: Callable[[CopyJob], CopyJob], jobs: Iterable[CopyJob]) -> list[CopyJob]:
    """Apply a transformation function to a collection of jobs.

    This is a small helper to encourage functional composition over
    in-place mutation.
    """

    return [transform(job) for job in jobs]
