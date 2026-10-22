"""Bronze = raw copy of the source, as Parquet, partitioned by ingestion date, plus lineage columns.

Rule: bronze never cleans, never filters, never changes a value. If the source lied, bronze keeps the lie
so we can always re-process silver from scratch.
"""

from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from .lake import Lake


def to_bronze(
    df: pl.DataFrame, lake: Lake, table: str, run_date: str, source: str
) -> tuple[str, int]:
    """Append metadata columns and write one Parquet file under bronze/<table>/dt=<run_date>/."""
    ingested_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    out = df.with_columns(
        pl.lit(ingested_at).alias("_ingested_at"),
        pl.lit(source).alias("_source"),
        pl.lit(run_date).alias("_run_date"),
    )
    uri = lake.write_parquet(out, "bronze", table, f"dt={run_date}")
    return uri, out.height
