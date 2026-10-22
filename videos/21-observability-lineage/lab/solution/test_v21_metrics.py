"""Test lab #21 — feed một summary dict → kiểm tra format Prometheus text. Offline, < 1 s."""

from __future__ import annotations

import re
from pathlib import Path

from metrics_exporter import format_labels, to_prometheus

from coffee_ingestion.pipeline import run

SAMPLES = Path(__file__).resolve().parents[4] / "data" / "samples"

# Summary thật của `coffee-ingest run --date 2026-02-15` trên data/samples (rút gọn phần bronze)
SUMMARY = {
    "run_date": "2026-02-15",
    "lake": "./lake-local",
    "bronze": {},
    "silver": {
        "pos_transactions": {
            "table": "pos_transactions",
            "rows_in": 799,
            "rows_out": 792,
            "quarantined": 4,
            "reasons": {"null_store": 4, "duplicate": 3, "tz_drift": 5},
        },
        "pos_line_items": {
            "table": "pos_line_items",
            "rows_in": 1401,
            "rows_out": 1392,
            "quarantined": 9,
            "reasons": {"negative_qty": 3, "orphan_txn": 6},
        },
        "stores": {"rows_in": 30, "rows_out": 30},
        "menu_items": {"rows_in": 23, "rows_out": 23},
        "customers": {"rows_in": 2000, "rows_out": 2000},
    },
}

SAMPLE_LINE = re.compile(r'^[a-z_]+\{(?:[a-z_]+="[^"]*",?)+\} -?\d+(\.\d+)?$')


def test_format_labels_keeps_order_and_quotes():
    assert format_labels({"table": "stores", "run_date": "2026-02-15"}) == (
        '{table="stores",run_date="2026-02-15"}'
    )


def test_expected_lines_present():
    lines = to_prometheus(SUMMARY)
    assert 'coffee_ingest_rows_in{table="pos_transactions",run_date="2026-02-15"} 799' in lines
    assert 'coffee_ingest_rows_out{table="pos_transactions",run_date="2026-02-15"} 792' in lines
    assert 'coffee_ingest_quarantined{table="pos_transactions",run_date="2026-02-15"} 4' in lines
    assert 'coffee_ingest_quarantined{table="stores",run_date="2026-02-15"} 0' in lines
    assert (
        'coffee_ingest_quarantined_reason{table="pos_line_items",reason="orphan_txn",run_date="2026-02-15"} 6'
        in lines
    )


def test_exposition_format_is_valid():
    lines = to_prometheus(SUMMARY)
    samples = [ln for ln in lines if not ln.startswith("#")]
    assert len(samples) == 20  # 5 bảng × 3 metric + 5 reason
    assert all(SAMPLE_LINE.match(ln) for ln in samples), samples
    # mỗi metric có đúng 1 cặp HELP/TYPE và các sample của nó nằm ngay sau header
    for metric in ("rows_in", "rows_out", "quarantined", "quarantined_reason"):
        name = f"coffee_ingest_{metric}"
        assert lines.count(f"# TYPE {name} gauge") == 1
        help_idx = next(i for i, ln in enumerate(lines) if ln.startswith(f"# HELP {name} "))
        assert lines[help_idx + 1] == f"# TYPE {name} gauge"
        assert lines[help_idx + 2].startswith(name + "{")


def test_real_pipeline_summary_matches_fixture(tmp_path: Path):
    """Chạy pipeline thật vào tmp_path: metric phải khớp con số ghi trong README."""
    summary = run(str(SAMPLES), str(tmp_path), "2026-02-15")
    lines = to_prometheus(summary)
    assert 'coffee_ingest_rows_out{table="pos_transactions",run_date="2026-02-15"} 792' in lines
    assert 'coffee_ingest_rows_out{table="pos_line_items",run_date="2026-02-15"} 1392' in lines
    assert len([ln for ln in lines if not ln.startswith("#")]) == 20
