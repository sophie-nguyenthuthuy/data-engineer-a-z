"""Test lab #07 — chạy offline trên data/samples (< 2 s)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
from extra_rule import apply_extra_rules

from coffee_ingestion.silver import clean_transactions
from coffee_ingestion.sources import read_folder

SAMPLES = Path(__file__).resolve().parents[4] / "data" / "samples"


def _silver(date: str | None) -> pl.DataFrame:
    raw = read_folder(SAMPLES, "pos_transactions")
    if date:
        raw = raw.filter(pl.col("ts").str.slice(0, 10) == date)
    silver, _, _ = clean_transactions(raw)
    return silver


def test_rule_on_2026_02_15_quarantines_exactly_one_row():
    silver = _silver("2026-02-15")
    clean, quarantine, reasons = apply_extra_rules(silver)
    assert silver.height == 792
    assert reasons == {"discount_gt_total": 1, "app_cash": 0}
    assert quarantine["transaction_id"].to_list() == ["HN01-20260215-00014"]
    assert clean.height == 791
    assert "_reason" not in clean.columns
    assert set(quarantine.columns) == {
        "transaction_id",
        "store_id",
        "channel",
        "payment_method",
        "total_amount_vnd",
        "discount_vnd",
        "_reason",
    }


def test_rule_on_all_days_and_nothing_is_lost():
    silver = _silver(None)
    clean, quarantine, reasons = apply_extra_rules(silver)
    assert reasons["discount_gt_total"] == 8
    assert reasons["app_cash"] == 0
    assert (
        clean.height + quarantine.height == silver.height
    )  # không xoá, chỉ chuyển sang quarantine


def test_app_cash_rule_fires_on_synthetic_row():
    silver = _silver("2026-02-17").with_columns(
        pl.when(pl.col("channel") == "app")
        .then(pl.lit("cash"))
        .otherwise(pl.col("payment_method"))
        .alias("payment_method")
    )
    _, quarantine, reasons = apply_extra_rules(silver)
    assert reasons["app_cash"] > 0
    assert (quarantine.filter(pl.col("_reason") == "app_cash")["channel"] == "app").all()
