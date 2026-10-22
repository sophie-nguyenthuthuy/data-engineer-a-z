"""Lời giải lab #21 — summary JSON của `coffee-ingest run` → Prometheus text (stdlib only).

    uv run coffee-ingest run --source data/samples --lake ./lake-local --date 2026-02-15 \
      | uv run python videos/21-observability-lineage/lab/solution/metrics_exporter.py
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PREFIX = "coffee_ingest"

METRICS = {
    "rows_in": "Rows read from source for this table",
    "rows_out": "Rows written to silver",
    "quarantined": "Rows moved to quarantine",
    "quarantined_reason": "Rows quarantined per reason",
}


def format_labels(labels: dict[str, str]) -> str:
    body = ",".join(f'{k}="{v}"' for k, v in labels.items())
    return "{" + body + "}"


def header(metric: str) -> list[str]:
    name = f"{PREFIX}_{metric}"
    return [f"# HELP {name} {METRICS[metric]}", f"# TYPE {name} gauge"]


def to_prometheus(summary: dict) -> list[str]:
    run_date = summary["run_date"]
    tables: dict[str, dict] = summary["silver"]
    lines: list[str] = []

    for metric in ("rows_in", "rows_out", "quarantined"):
        lines += header(metric)
        for table, stats in tables.items():
            labels = format_labels({"table": table, "run_date": run_date})
            lines.append(f"{PREFIX}_{metric}{labels} {stats.get(metric, 0)}")

    lines += header("quarantined_reason")
    for table, stats in tables.items():
        for reason, n in stats.get("reasons", {}).items():
            labels = format_labels({"table": table, "reason": reason, "run_date": run_date})
            lines.append(f"{PREFIX}_quarantined_reason{labels} {n}")
    return lines


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--file", default="-", help="file summary JSON, mặc định đọc stdin")
    a = p.parse_args()
    text = sys.stdin.read() if a.file == "-" else Path(a.file).read_text(encoding="utf-8")
    print("\n".join(to_prometheus(json.loads(text))))
    return 0


if __name__ == "__main__":
    sys.exit(main())
