"""Test lab #11 (small files) — offline, < 10 s."""

from __future__ import annotations

from pathlib import Path

import polars as pl
from small_files import load_source, run


def test_many_small_files_cost_more_bytes_and_time(tmp_path: Path):
    r = run(tmp_path, n_files=500, repeat=2)
    assert r["rows"] == 2481
    assert r["one_file"]["files"] == 1
    assert r["many_files"]["files"] == 500
    assert r["compacted"]["files"] == 1
    # footer + dictionary lặp lại 500 lần → tổng bytes phình ra nhiều lần
    assert r["bytes_overhead_x"] >= 5
    # đọc 500 file chậm hơn đọc 1 file (ngưỡng thấp để không flaky trên CI)
    assert r["polars_slowdown_x"] >= 2
    # compact lại cho kích thước ~ file gốc
    assert r["compacted"]["bytes"] < r["one_file"]["bytes"] * 1.2


def test_many_files_hold_exactly_the_same_data(tmp_path: Path):
    run(tmp_path, n_files=50, repeat=1)
    df = load_source()
    many = pl.scan_parquet(str(tmp_path / "many" / "*.parquet")).collect()
    assert many.height == df.height
    assert many["total_amount_vnd"].sum() == df["total_amount_vnd"].sum()
