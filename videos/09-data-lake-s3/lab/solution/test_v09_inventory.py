"""Test lab #09 — tạo lake trong tmp_path bằng coffee_ingestion.pipeline.run rồi kiểm kê (offline, < 3 s)."""

from __future__ import annotations

from pathlib import Path

from lake_inventory import inventory, summarize

from coffee_ingestion.pipeline import run

SAMPLES = Path(__file__).resolve().parents[4] / "data" / "samples"


def test_inventory_on_fresh_local_lake(tmp_path: Path):
    lake = tmp_path / "lake"
    run(str(SAMPLES), str(lake), "2026-02-15")
    rows = inventory(str(lake))

    by_key = {(r["layer"], r["table"], r["partition"]): r for r in rows}
    assert by_key[("bronze", "pos_transactions", "dt=2026-02-15")]["rows"] == 799
    assert by_key[("silver", "pos_transactions", "dt=2026-02-15")]["rows"] == 792
    assert by_key[("silver", "quarantine_pos_transactions", "dt=2026-02-15")]["rows"] == 4
    assert all(r["files"] == 1 and r["bytes"] > 0 for r in rows)

    layers = summarize(rows)
    assert set(layers) == {"bronze", "silver"}  # gold do dbt tạo (video #12), chưa có ở đây
    assert layers["bronze"]["tables"] == 5
    assert layers["silver"]["tables"] == 7
    assert layers["bronze"]["files"] == 5 and layers["silver"]["files"] == 7


def test_second_day_adds_partitions_not_files_per_partition(tmp_path: Path):
    lake = tmp_path / "lake"
    run(str(SAMPLES), str(lake), "2026-02-15")
    run(str(SAMPLES), str(lake), "2026-02-16")
    run(str(SAMPLES), str(lake), "2026-02-16")  # re-run cùng ngày: ghi đè, không thêm file
    rows = inventory(str(lake), count_rows=False)
    pos = [r for r in rows if r["table"] == "pos_transactions" and r["layer"] == "silver"]
    assert [r["partition"] for r in pos] == ["dt=2026-02-15", "dt=2026-02-16"]
    assert all(r["files"] == 1 for r in pos)


def test_missing_lake_is_empty_not_error(tmp_path: Path):
    assert inventory(str(tmp_path / "nothing-here")) == []
