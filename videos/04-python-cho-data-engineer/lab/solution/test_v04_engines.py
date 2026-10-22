"""Test lời giải #04 — 3 engine phải ra cùng kết quả trên data/samples. < 3 s, không cần Docker."""

from __future__ import annotations

from datetime import date

import pytest
from compare_engines import SAMPLES, revenue_duckdb, revenue_pandas, revenue_polars

D15 = date(2026, 2, 15)


@pytest.fixture(scope="module")
def results() -> dict[str, list[tuple[str, int]]]:
    return {
        "pandas": revenue_pandas(SAMPLES, D15),
        "polars": revenue_polars(SAMPLES, D15),
        "duckdb": revenue_duckdb(SAMPLES, D15),
    }


def test_three_engines_agree(results):
    assert results["pandas"] == results["polars"] == results["duckdb"]


def test_numbers_are_the_real_ones(results):
    out = dict(results["duckdb"])
    assert len(out) == 30  # đủ 30 cửa hàng, không còn store_id NULL
    # chưa dedup: 795 hoá đơn, 77 113 000 VND (silver sau dedup là 76 939 000)
    assert sum(out.values()) == 77_113_000
    assert out["SG14"] == 4_776_000
    assert out["SG04"] == 3_607_000


def test_tz_drift_is_handled():
    # Ngày 14 có hoá đơn HN07 21:06 thiếu offset; đọc đúng thì nó ở ngày 14, không trôi sang ngày 15.
    out14 = dict(revenue_duckdb(SAMPLES, date(2026, 2, 14)))
    assert sum(out14.values()) == 73_451_000
    assert revenue_polars(SAMPLES, date(2026, 2, 14)) == sorted(out14.items())
