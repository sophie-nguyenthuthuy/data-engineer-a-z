"""`coffee-ingest` CLI."""

from __future__ import annotations

import argparse
import sys

from .pipeline import print_summary, run


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="coffee-ingest", description="Batch ingestion: source → bronze → silver"
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run bronze + silver for one date")
    r.add_argument(
        "--source", default="data/samples", help="folder of CSVs or postgresql://user:pw@host/db"
    )
    r.add_argument(
        "--lake", default="./lake-local", help="./folder or s3://bucket (S3_ENDPOINT for MinIO)"
    )
    r.add_argument("--date", required=True, help="business date YYYY-MM-DD")
    r.add_argument(
        "--all-dates",
        action="store_true",
        help="do not filter by --date (load every row into that partition)",
    )
    a = p.parse_args(argv)
    summary = run(a.source, a.lake, a.date, filter_by_date=not a.all_dates)
    print_summary(summary)
    s = summary["silver"]["pos_transactions"]
    if s["rows_out"] == 0:
        print(
            "⚠️  silver.pos_transactions trống — kiểm tra --date có nằm trong dữ liệu nguồn không",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
