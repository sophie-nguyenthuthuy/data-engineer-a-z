"""The first pipeline of the course (video #07): source → bronze → silver, one run date at a time.

    coffee-ingest run --source data/samples --lake ./lake-local --date 2026-02-16

Idempotent: re-running the same date overwrites the same partition (no duplicates from re-runs).
"""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

import polars as pl

from .bronze import to_bronze
from .lake import Lake
from .silver import clean_dimension, clean_line_items, clean_transactions
from .sources import POS_TABLES, read_folder, read_postgres


def load_sources(source: str) -> dict[str, pl.DataFrame]:
    if source.startswith("postgresql://"):
        return {t: read_postgres(source, t) for t in POS_TABLES}
    folder = Path(source)
    return {t: read_folder(folder, t) for t in POS_TABLES}


def run(source: str, lake_root: str, run_date: str, filter_by_date: bool = True) -> dict:
    """Run bronze + silver for one date. Returns a JSON-serialisable summary (used by Airflow and tests)."""
    lake = Lake(lake_root)
    raw = load_sources(source)
    summary: dict = {"run_date": run_date, "lake": lake_root, "bronze": {}, "silver": {}}

    # ── bronze: land everything as-is ──
    for table, df in raw.items():
        if filter_by_date and table == "pos_transactions":
            # one partition per business day; naive strings compare fine on the first 10 chars
            df = df.filter(pl.col("ts").str.slice(0, 10) == run_date)
        if filter_by_date and table == "pos_line_items":
            txn_ids = raw["pos_transactions"].filter(pl.col("ts").str.slice(0, 10) == run_date)[
                "transaction_id"
            ]
            df = df.filter(pl.col("transaction_id").is_in(txn_ids.implode()))
        uri, n = to_bronze(df, lake, table, run_date, source)
        summary["bronze"][table] = {"rows": n, "path": uri}
        raw[table] = df

    # ── silver: type, dedup, quarantine ──
    txns, q_txn, s_txn = clean_transactions(raw["pos_transactions"])
    lines, q_lines, s_lines = clean_line_items(raw["pos_line_items"], txns["transaction_id"])
    part = f"dt={run_date}"
    lake.write_parquet(txns, "silver", "pos_transactions", part)
    lake.write_parquet(lines, "silver", "pos_line_items", part)
    lake.write_parquet(q_txn, "silver", "quarantine_pos_transactions", part)
    lake.write_parquet(q_lines, "silver", "quarantine_pos_line_items", part)
    summary["silver"]["pos_transactions"] = asdict(s_txn)
    summary["silver"]["pos_line_items"] = asdict(s_lines)

    dims = {
        "stores": ("store_id", {"seats": pl.Int64, "tier": pl.Float64, "opened_date": pl.Date}),
        "menu_items": ("item_id", {"price_vnd": pl.Int64, "cost_vnd": pl.Int64}),
        "customers": ("customer_id", {"joined_date": pl.Date}),
    }
    for table, (key, casts) in dims.items():
        df = clean_dimension(raw[table], key, casts)
        lake.write_parquet(df, "silver", table, part)
        summary["silver"][table] = {"rows_in": raw[table].height, "rows_out": df.height}
    return summary


def print_summary(summary: dict) -> None:
    print(json.dumps(summary, ensure_ascii=False, indent=2))
