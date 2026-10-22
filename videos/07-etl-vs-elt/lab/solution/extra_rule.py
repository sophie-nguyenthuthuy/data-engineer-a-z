"""Lời giải lab #07 — rule silver bổ sung: discount_gt_total và app_cash.

Chạy:
    uv run python videos/07-etl-vs-elt/lab/solution/extra_rule.py --date 2026-02-15
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import polars as pl

from coffee_ingestion.silver import clean_transactions
from coffee_ingestion.sources import read_folder

REPO = Path(__file__).resolve().parents[4]
SAMPLES = REPO / "data" / "samples"

QUARANTINE_COLS = (
    "transaction_id",
    "store_id",
    "channel",
    "payment_method",
    "total_amount_vnd",
    "discount_vnd",
    "_reason",
)


def apply_extra_rules(silver: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, int]]:
    """Nhận silver (đã typed, đã dedup) → trả (silver_sạch, quarantine, số dòng theo reason)."""
    discount_gt_total = pl.col("discount_vnd") > pl.col("total_amount_vnd")
    app_cash = (pl.col("channel") == "app") & (pl.col("payment_method") == "cash")

    flagged = silver.with_columns(
        pl.when(discount_gt_total)
        .then(pl.lit("discount_gt_total"))
        .when(app_cash)
        .then(pl.lit("app_cash"))
        .otherwise(pl.lit(None, dtype=pl.Utf8))
        .alias("_reason")
    )
    quarantine = flagged.filter(pl.col("_reason").is_not_null()).select(*QUARANTINE_COLS)
    clean = flagged.filter(pl.col("_reason").is_null()).drop("_reason")

    reasons = {"discount_gt_total": 0, "app_cash": 0}
    for reason, n in quarantine.group_by("_reason").len().iter_rows():
        reasons[reason] = n
    return clean, quarantine, reasons


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", default=str(SAMPLES))
    p.add_argument("--date", default="2026-02-15", help="YYYY-MM-DD; 'all' = không lọc")
    a = p.parse_args()

    raw = read_folder(Path(a.source), "pos_transactions")
    if a.date != "all":
        raw = raw.filter(pl.col("ts").str.slice(0, 10) == a.date)
    silver, _q_builtin, stats = clean_transactions(raw)
    clean, quarantine, reasons = apply_extra_rules(silver)

    print(
        json.dumps(
            {
                "date": a.date,
                "builtin": stats.reasons,
                "rows_in": silver.height,
                "rows_out": clean.height,
                "extra_quarantined": quarantine.height,
                "extra_reasons": reasons,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if quarantine.height:
        print(quarantine)


if __name__ == "__main__":
    main()
