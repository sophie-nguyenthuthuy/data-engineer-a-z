"""Lab #21 — đổi summary JSON của `coffee-ingest run` thành metric dạng Prometheus text (stdlib only).

Chạy:
    uv run coffee-ingest run --source data/samples --lake ./lake-local --date 2026-02-15 \
      | uv run python videos/21-observability-lineage/lab/metrics_exporter.py
    # hoặc: ... metrics_exporter.py --file summary.json

Format Prometheus text (https://prometheus.io/docs/instrumenting/exposition_formats/):
    # HELP <name> <mô tả>
    # TYPE <name> gauge
    <name>{label="value",label2="value2"} <số>

Summary đầu vào (xem coffee_ingestion/pipeline.py) có dạng:
    {"run_date": "2026-02-15",
     "silver": {"pos_transactions": {"rows_in": 799, "rows_out": 792, "quarantined": 4,
                                     "reasons": {"null_store": 4, "duplicate": 3, "tz_drift": 5}},
                "stores": {"rows_in": 30, "rows_out": 30}, ...}}
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PREFIX = "coffee_ingest"

# metric name → mô tả cho dòng # HELP
METRICS = {
    "rows_in": "Rows read from source for this table",
    "rows_out": "Rows written to silver",
    "quarantined": "Rows moved to quarantine",
    "quarantined_reason": "Rows quarantined per reason",
}


def format_labels(labels: dict[str, str]) -> str:
    """{"table": "stores", "run_date": "2026-02-15"} → '{table="stores",run_date="2026-02-15"}'."""
    # TODO: nối từng cặp key="value" bằng dấu phẩy, bọc trong {} — thứ tự giữ nguyên như dict.
    raise NotImplementedError


def header(metric: str) -> list[str]:
    """Hai dòng # HELP và # TYPE cho một metric (TYPE luôn là gauge: mỗi run là một snapshot)."""
    # TODO
    raise NotImplementedError


def to_prometheus(summary: dict) -> list[str]:
    """Trả về danh sách dòng text. Metric cùng tên phải nằm cạnh nhau sau header của nó."""
    # TODO: run_date = summary["run_date"]; lines: list[str] = []
    # TODO: với mỗi metric trong ("rows_in", "rows_out", "quarantined"):
    #   - lines += header(metric)
    #   - với mỗi table, stats trong summary["silver"].items(): giá trị = stats.get(metric, 0)
    #     label: table=<tên bảng>, run_date=<run_date>  → f"{PREFIX}_{metric}{labels} {value}"
    # TODO: quarantined_reason — với mỗi table có "reasons": một dòng / reason,
    #     label: table, reason, run_date
    # return lines
    raise NotImplementedError


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--file", default="-", help="file summary JSON, mặc định đọc stdin")
    a = p.parse_args()
    text = sys.stdin.read() if a.file == "-" else Path(a.file).read_text(encoding="utf-8")
    summary = json.loads(text)
    print("\n".join(to_prometheus(summary)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
