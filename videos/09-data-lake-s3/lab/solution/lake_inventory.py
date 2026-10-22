"""Lời giải lab #09 — kiểm kê data lake: partition, số file, dung lượng, số dòng cho từng layer/table.

Chạy với MinIO (sau `make up` và đã ingest bằng --lake s3://lake):
    S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
      uv run python videos/09-data-lake-s3/lab/solution/lake_inventory.py --lake s3://lake
Chạy offline với thư mục local:
    uv run python videos/09-data-lake-s3/lab/solution/lake_inventory.py --lake ./lake-local

Dùng lại `Lake` của platform/ingestion để không viết lại phần kết nối S3; pyarrow.fs cho local và S3 cùng một API.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq
from pyarrow import fs

from coffee_ingestion.lake import LAYERS, Lake


def open_filesystem(lake: Lake) -> tuple[fs.FileSystem, str]:
    """(filesystem, đường dẫn gốc không có scheme). Local → LocalFileSystem; s3:// → S3FileSystem của Lake."""
    if lake.is_s3:
        return lake._fs(), lake.root.removeprefix("s3://").rstrip("/")  # noqa: SLF001 — helper nội bộ của khoá
    return fs.LocalFileSystem(), str(Path(lake.root).resolve())


def list_parquet_files(filesystem: fs.FileSystem, base: str) -> list[fs.FileInfo]:
    """Mọi file .parquet dưới base (đệ quy). Trên S3 đây là MỘT lệnh ListObjectsV2 — không tải dữ liệu."""
    selector = fs.FileSelector(base, recursive=True, allow_not_found=True)
    return [
        f
        for f in filesystem.get_file_info(selector)
        if f.type == fs.FileType.File and f.path.endswith(".parquet")
    ]


def inventory(lake_root: str, count_rows: bool = True) -> list[dict]:
    """Một dict cho mỗi (layer, table, partition): files, bytes, rows."""
    lake = Lake(lake_root)
    filesystem, base = open_filesystem(lake)
    acc: dict[tuple[str, str, str], dict] = defaultdict(lambda: {"files": 0, "bytes": 0, "rows": 0})

    for info in list_parquet_files(filesystem, base):
        rel = info.path.removeprefix(base).strip(
            "/"
        )  # bronze/pos_transactions/dt=2026-02-15/part-000.parquet
        parts = rel.split("/")
        if len(parts) < 3 or parts[0] not in LAYERS:
            continue
        layer, table = parts[0], parts[1]
        partition = "/".join(parts[2:-1]) or "-"
        row = acc[(layer, table, partition)]
        row["files"] += 1
        row["bytes"] += info.size or 0
        if count_rows:
            # Chỉ đọc footer (vài KB), không đọc dữ liệu — vì sao biết? xem video #08.
            row["rows"] += pq.ParquetFile(info.path, filesystem=filesystem).metadata.num_rows

    return [
        {"layer": layer, "table": table, "partition": part, **vals}
        for (layer, table, part), vals in sorted(acc.items())
    ]


def summarize(rows: list[dict]) -> dict[str, dict]:
    totals: dict[str, dict] = defaultdict(
        lambda: {"tables": set(), "partitions": 0, "files": 0, "bytes": 0, "rows": 0}
    )
    for r in rows:
        t = totals[r["layer"]]
        t["tables"].add(r["table"])
        t["partitions"] += 1
        for k in ("files", "bytes", "rows"):
            t[k] += r[k]
    return {layer: {**t, "tables": len(t["tables"])} for layer, t in totals.items()}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--lake", default="./lake-local", help="./lake-local hoặc s3://lake (cần S3_ENDPOINT…)"
    )
    p.add_argument("--no-rows", action="store_true", help="bỏ qua đếm dòng (nhanh hơn trên S3)")
    p.add_argument("--json", action="store_true")
    a = p.parse_args()

    rows = inventory(a.lake, count_rows=not a.no_rows)
    if a.json:
        print(json.dumps({"lake": a.lake, "partitions": rows, "layers": summarize(rows)}, indent=2))
        return

    print(f"{'layer':8}{'table':32}{'partition':16}{'files':>6}{'bytes':>10}{'rows':>8}")
    for r in rows:
        print(
            f"{r['layer']:8}{r['table']:32}{r['partition']:16}{r['files']:>6}{r['bytes']:>10,}{r['rows']:>8,}"
        )
    print()
    for layer, t in summarize(rows).items():
        print(
            f"{layer:8} tables={t['tables']} partitions={t['partitions']} files={t['files']} "
            f"bytes={t['bytes']:,} rows={t['rows']:,}"
        )


if __name__ == "__main__":
    main()
