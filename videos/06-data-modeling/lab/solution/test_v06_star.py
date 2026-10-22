"""Test lời giải #06 — load star schema vào DuckDB từ data/samples, so với silver của coffee_ingestion. < 5 s, không Docker."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import polars as pl
import pytest

from coffee_ingestion.silver import clean_line_items, clean_transactions
from coffee_ingestion.sources import read_csv_raw

LAB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB))  # load_star.py nằm ở lab/
from load_star import load, table_counts  # noqa: E402

REPO = Path(__file__).resolve().parents[4]
SAMPLES = REPO / "data" / "samples"
HERE = Path(__file__).parent


@pytest.fixture(scope="module")
def star() -> duckdb.DuckDBPyConnection:
    return load(HERE / "star_schema.sql")


@pytest.fixture(scope="module")
def silver_lines() -> pl.DataFrame:
    """Cùng rule làm sạch nhưng bằng code thật của platform — nguồn sự thật để so."""
    txns, _, _ = clean_transactions(read_csv_raw(SAMPLES / "pos_transactions.csv"))
    lines, _, _ = clean_line_items(
        read_csv_raw(SAMPLES / "pos_line_items.csv"), txns["transaction_id"]
    )
    return lines


def test_dimension_counts(star):
    counts = table_counts(star)
    assert counts["dim_store"] == 30
    assert counts["dim_menu_item"] == 23
    assert counts["dim_customer"] == 2000
    assert counts["dim_date"] == 1095  # 2025-01-01 → 2027-12-31


def test_fact_matches_silver(star, silver_lines):
    n, revenue, margin = star.execute(
        "SELECT count(*), sum(line_total_vnd), sum(line_margin_vnd) FROM fct_sales"
    ).fetchall()[0]
    assert n == silver_lines.height == 4302  # số line item hợp lệ
    assert revenue == int(silver_lines["line_total_vnd"].sum()) == 240_469_000
    assert margin == 181_623_000


def test_fact_grain_and_keys(star):
    dup = star.execute("SELECT count(*) - count(DISTINCT line_id) FROM fct_sales").fetchall()[0][0]
    assert dup == 0
    orphans = star.execute(
        "SELECT count(*) FROM fct_sales f LEFT JOIN dim_store s USING (store_id) WHERE s.store_id IS NULL"
    ).fetchall()[0][0]
    assert orphans == 0
    tz_fixed = star.execute(
        "SELECT count(*) FROM fct_sales WHERE business_date = DATE '2026-02-14' AND transaction_id = 'HN07-20260214-00014'"
    ).fetchall()[0][0]
    assert tz_fixed >= 1  # hoá đơn 21:06 thiếu offset vẫn nằm đúng ngày 14


def test_dim_date_tet_flags(star):
    rows = star.execute(
        "SELECT date_day, is_weekend, is_tet_season, is_tet_holiday FROM dim_date "
        "WHERE date_day IN (DATE '2026-02-15', DATE '2026-02-17', DATE '2026-03-03') ORDER BY 1"
    ).fetchall()
    assert [r[1:] for r in rows] == [
        (True, True, False),
        (False, True, True),
        (False, False, False),
    ]


def test_business_question_runs(star):
    rows = star.execute(
        "SELECT channel, sum(revenue_vnd), sum(margin_vnd) FROM rpt_revenue_margin "
        "WHERE business_date = DATE '2026-02-15' GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert rows == [("app", 29_881_000, 22_587_000), ("store", 47_972_000, 36_246_000)]


def test_data_vault_example():
    con = load(HERE / "data_vault.sql")
    counts = table_counts(con)
    assert counts["hub_store"] == 30 and counts["hub_machine"] == 60
    assert counts["link_store_machine"] == 60  # 2 máy / cửa hàng
    assert counts["dim_store_from_vault"] == 30
