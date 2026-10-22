"""Test offline cho #17: đọc metadata Iceberg (fixture tối giản theo spec v2) bằng metadata_explorer."""

from __future__ import annotations

from pathlib import Path

import pytest
from metadata_explorer import find_current_metadata, load_metadata, render, summarize

FIXTURE = Path(__file__).parent / "fixtures" / "metadata"
FIRST, SECOND = 3051729663735987200, 8821937160440015002


def test_version_hint_points_to_current_metadata(tmp_path: Path) -> None:
    assert find_current_metadata(FIXTURE).name == "v2.metadata.json"
    # không có hint → lấy N lớn nhất
    for n in (1, 2, 10):
        (tmp_path / f"v{n}.metadata.json").write_text("{}")
    assert find_current_metadata(tmp_path).name == "v10.metadata.json"
    with pytest.raises(FileNotFoundError):
        find_current_metadata(tmp_path / "empty")


def test_snapshots_summary() -> None:
    meta = load_metadata(find_current_metadata(FIXTURE))
    assert meta["format-version"] == 2
    assert meta["current-snapshot-id"] == SECOND

    rows = summarize(meta)
    assert [r["snapshot_id"] for r in rows] == [FIRST, SECOND]
    assert [r["operation"] for r in rows] == ["append", "overwrite"]
    assert [r["added_records"] for r in rows] == [792, 792]
    assert [r["deleted_records"] for r in rows] == [0, 792]  # MERGE lần 2 = copy-on-write
    assert all(r["total_records"] == 792 for r in rows)
    assert rows[1]["parent_id"] == FIRST
    assert [r["is_current"] for r in rows] == [False, True]
    assert rows[0]["committed_at"].isoformat() == "2026-02-15T06:00:00+00:00"
    assert rows[1]["manifest_list"].endswith(".avro")


def test_render_marks_current_snapshot() -> None:
    meta = load_metadata(find_current_metadata(FIXTURE))
    out = render(meta, summarize(meta))
    assert "*8821937160440015002" in out
    assert " 3051729663735987200" in out
    assert "overwrite" in out and "append" in out
