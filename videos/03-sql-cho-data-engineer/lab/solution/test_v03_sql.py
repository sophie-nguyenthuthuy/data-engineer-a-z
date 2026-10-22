"""Test lời giải #03 — chạy solution/queries.sql trên DuckDB với data/samples, assert số dòng thật. < 3 s, không cần Docker."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

LAB = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAB))  # run_sql.py nằm ở lab/, không phải lab/solution/
from run_sql import run_file  # noqa: E402

SOLUTION_SQL = Path(__file__).with_name("queries.sql")


@pytest.fixture(scope="module")
def results() -> dict[str, list[tuple]]:
    return run_file(SOLUTION_SQL, verbose=False)


def test_all_blocks_ran(results):
    expected = {"Q1a", "Q1b", "Q1c", "Q1d", "Q2", "Q3", "Q4a", "Q4b", "Q5a", "Q5b", "Q6"}
    assert expected <= set(results), f"thiếu: {expected - set(results)}"


def test_q1_dedup(results):
    assert len(results["Q1a"]) == 5  # 5 transaction_id xuất hiện 2 lần
    assert results["Q1b"] == [(2481, 2476)]
    by_day = {str(d): (n, rev) for d, n, rev in results["Q1d"]}
    assert by_day["2026-02-15"] == (792, 76_939_000)
    assert by_day["2026-02-14"] == (766, 73_397_000)
    assert by_day["2026-02-17"] == (59, 5_549_000)  # mùng 1 Tết


def test_q2_running_total(results):
    rows = results["Q2"]
    assert len(rows) == 180  # 30 cửa hàng × 6 ngày
    hn01 = [r for r in rows if r[0] == "HN01"]
    assert [r[3] for r in hn01] == [
        1_111_000,
        2_693_000,
        3_835_000,
        3_925_000,
        4_075_000,
        4_110_000,
    ]


def test_q3_top3(results):
    rows = results["Q3"]
    assert len(rows) == 90  # 3 món × 30 cửa hàng, không có hoà điểm trong samples
    hn01 = [(r[2], r[4]) for r in rows if r[0] == "HN01"]
    assert hn01 == [("TE01", 471_000), ("TE02", 452_000), ("CF02", 380_000)]


def test_q4_calendar(results):
    hours = dict(results["Q4a"])
    assert len(hours) == 24
    assert all(hours[h] == 0 for h in (0, 1, 2, 3, 4, 23))  # quán đóng cửa
    assert hours[8] == 87  # giờ cao điểm sáng
    assert len(results["Q4b"]) == 6


def test_q5_tz_drift(results):
    assert len(results["Q5a"]) == 11
    assert all(" " in r[2] and "+07:00" not in r[2] for r in results["Q5a"])
    # Q5b: dòng HN07 21:06 ngày 14 — đọc sai thành 04:06 ngày 15, sửa đúng vẫn là 21:06 ngày 14
    hn07 = next(r for r in results["Q5b"] if r[0] == "HN07-20260214-00014")
    assert str(hn07[2]).startswith("2026-02-15 04:06")
    assert str(hn07[3]).startswith("2026-02-14 21:06")


def test_q6_explain_runs(results):
    plan = results["Q6"][0][1]
    assert "Table Scan" in plan or "TABLE_SCAN" in plan.upper()
