"""Utility time-series analytics package for QuestDB-backed electric utility data.

This package is structured in a functional style: modules expose pure functions
for composing ingestion pipelines and analytical queries.
"""

__all__ = [
    "config",
    "ingest",
    "queries",
]
