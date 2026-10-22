"""Test lab #08 — offline, < 5 s."""

from __future__ import annotations

from pathlib import Path

import polars as pl
from formats_bench import (
    ONE_COLUMN,
    load_source,
    parquet_metadata,
    pushdown_demo,
    readers,
    write_all,
)


def test_parquet_is_smaller_than_csv_and_jsonl(tmp_path: Path):
    files = write_all(load_source(), tmp_path)
    size = {k: v.stat().st_size for k, v in files.items()}
    assert size["parquet_zstd"] < size["parquet_snappy"] < size["csv"] < size["jsonl"]


def test_reading_one_column_matches_full_table(tmp_path: Path):
    df = load_source()
    files = write_all(df, tmp_path)
    expected = df[ONE_COLUMN].sum()
    for fmt, path in files.items():
        read_all, read_one = readers(path)
        one = read_one()
        assert one.columns == [ONE_COLUMN], fmt
        assert one[ONE_COLUMN].sum() == expected, fmt
        assert read_all().height == df.height, fmt


def test_parquet_footer_has_row_group_statistics(tmp_path: Path):
    files = write_all(load_source(), tmp_path)
    md = parquet_metadata(files["parquet_zstd"])
    assert md["num_rows"] == 2481
    assert md["num_row_groups"] >= 1
    stats = md["row_groups"][0]["stats"]
    assert stats["total_amount_vnd"]["codec"] == "ZSTD"
    assert int(stats["total_amount_vnd"]["min"]) >= 0


def test_pushdown_skips_row_groups(tmp_path: Path):
    df = load_source()
    out = pushdown_demo(df, tmp_path)
    assert out["rows_SG14"] == df.filter(pl.col("store_id") == "SG14").height
    assert 0 < len(out["row_groups_needed_for_SG14"]) < out["row_groups_total"]
