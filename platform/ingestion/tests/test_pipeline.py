"""Pipeline tests run on the committed samples — no Docker needed."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from coffee_ingestion.cli import main
from coffee_ingestion.lake import Lake
from coffee_ingestion.silver import clean_line_items, clean_transactions, parse_vn_timestamp

REPO = Path(__file__).resolve().parents[3]
SAMPLES = REPO / "data" / "samples"


def test_parse_vn_timestamp_handles_missing_offset():
    df = pl.DataFrame({"ts": ["2026-02-16T08:00:00+07:00", "2026-02-16 08:00:00"]})
    out = df.select(parse_vn_timestamp("ts").alias("ts"))["ts"].to_list()
    assert out[0] == out[1], "naive timestamp must be read as Vietnam local time"
    assert out[0].hour == 1  # 08:00 +07:00 == 01:00 UTC


def test_clean_transactions_removes_dups_and_null_store():
    raw = pl.read_csv(SAMPLES / "pos_transactions.csv", infer_schema=False, null_values=[""])
    silver, quarantine, stats = clean_transactions(raw)
    assert silver["transaction_id"].n_unique() == silver.height
    assert silver["store_id"].null_count() == 0
    assert (
        quarantine.height > 0 and stats.reasons["duplicate"] > 0 and stats.reasons["tz_drift"] > 0
    )
    assert str(silver.schema["ts"]) == "Datetime(time_unit='us', time_zone='UTC')"


def test_clean_line_items_quarantines_negative_qty():
    raw_t = pl.read_csv(SAMPLES / "pos_transactions.csv", infer_schema=False, null_values=[""])
    raw_l = pl.read_csv(SAMPLES / "pos_line_items.csv", infer_schema=False, null_values=[""])
    silver_t, _, _ = clean_transactions(raw_t)
    silver_l, q, stats = clean_line_items(raw_l, silver_t["transaction_id"])
    assert (silver_l["quantity"] > 0).all()
    assert stats.reasons["negative_qty"] > 0
    assert set(q["_reason"].unique()) <= {"negative_qty", "orphan_txn"}


def test_end_to_end_local_lake(tmp_path: Path, capsys):
    rc = main(
        ["run", "--source", str(SAMPLES), "--lake", str(tmp_path / "lake"), "--date", "2026-02-16"]
    )
    assert rc == 0
    lake = Lake(str(tmp_path / "lake"))
    bronze = lake.read_parquet("bronze", "pos_transactions", "dt=2026-02-16")
    silver = lake.read_parquet("silver", "pos_transactions", "dt=2026-02-16")
    assert bronze.height > silver.height > 100
    assert {"_ingested_at", "_source", "_run_date"} <= set(bronze.columns)
    assert bronze.schema["ts"] == pl.Utf8  # bronze keeps strings
    # idempotent: second run overwrites, same row count
    main(
        ["run", "--source", str(SAMPLES), "--lake", str(tmp_path / "lake"), "--date", "2026-02-16"]
    )
    assert lake.read_parquet("silver", "pos_transactions", "dt=2026-02-16").height == silver.height


def test_missing_date_returns_2(tmp_path: Path):
    assert (
        main(["run", "--source", str(SAMPLES), "--lake", str(tmp_path), "--date", "2030-01-01"])
        == 2
    )


@pytest.mark.parametrize(
    "layer,table", [("silver", "stores"), ("silver", "menu_items"), ("silver", "customers")]
)
def test_dimensions_land_in_silver(tmp_path: Path, layer: str, table: str):
    main(["run", "--source", str(SAMPLES), "--lake", str(tmp_path), "--date", "2026-02-16"])
    assert Lake(str(tmp_path)).read_parquet(layer, table, "dt=2026-02-16").height > 0


def test_write_parquet_overwrites_whole_partition(tmp_path):
    """Overwrite = xoá file cũ trong partition (kể cả file Spark để lại), không chỉ ghi đè part-000."""
    import polars as pl

    from coffee_ingestion.lake import Lake

    lake = Lake(str(tmp_path))
    part = tmp_path / "silver" / "t" / "dt=2026-02-15"
    part.mkdir(parents=True)
    (part / "part-00000-spark.snappy.parquet").write_bytes(b"old")
    (part / "_SUCCESS").write_bytes(b"")
    lake.write_parquet(pl.DataFrame({"a": [1]}), "silver", "t", "dt=2026-02-15")
    assert sorted(p.name for p in part.iterdir()) == ["part-000.parquet"]


def test_write_parquet_relative_root_keeps_new_file(tmp_path, monkeypatch):
    """Root tương đối kiểu './lake-local' (như `make smoke`) không được xoá nhầm file vừa ghi."""
    import polars as pl

    from coffee_ingestion.lake import Lake

    monkeypatch.chdir(tmp_path)
    lake = Lake("./lake-local")
    uri = lake.write_parquet(pl.DataFrame({"a": [1, 2]}), "silver", "t", "dt=2026-02-15")
    assert Path(uri).exists()
    assert pl.read_parquet(uri).height == 2
