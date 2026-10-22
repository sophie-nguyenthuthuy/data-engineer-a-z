"""Lời giải lab #17 — đọc thư mục metadata/ của một bảng Iceberg (hadoop catalog) bằng stdlib, in snapshots.

    uv run python videos/17-lakehouse-iceberg-delta-hudi/lab/solution/metadata_explorer.py <thư mục metadata/>
Ví dụ với fixture:  ... metadata_explorer.py videos/17-lakehouse-iceberg-delta-hudi/lab/solution/fixtures/metadata
Với bảng thật trên MinIO:
    docker compose exec minio mc cp -r local/warehouse/silver/pos_transactions/metadata /tmp/meta
    docker compose cp minio:/tmp/meta ./tmp-meta && uv run python .../metadata_explorer.py ./tmp-meta

Hadoop catalog: version-hint.text chứa N → vN.metadata.json là metadata hiện hành. File .avro (manifest list,
manifest) cần thư viện Avro nên lab chỉ đọc JSON — đủ để thấy snapshots và time travel hoạt động thế nào.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path


def find_current_metadata(meta_dir: Path) -> Path:
    """Hadoop catalog: version-hint.text → vN.metadata.json.
    JDBC/REST catalog: không có hint, file tên NNNNN-<uuid>.metadata.json → lấy số thứ tự lớn nhất.
    """
    hint = meta_dir / "version-hint.text"
    if hint.exists():
        n = int(hint.read_text().strip())
        return meta_dir / f"v{n}.metadata.json"
    candidates = sorted(
        (p for p in meta_dir.glob("*.metadata.json") if re.match(r"v?(\d+)", p.name)),
        key=lambda p: int(re.match(r"v?(\d+)", p.name).group(1)),
    )
    if not candidates:
        raise FileNotFoundError(f"không có *.metadata.json trong {meta_dir}")
    return candidates[-1]


def load_metadata(path: Path) -> dict:
    meta = json.loads(path.read_text(encoding="utf-8"))
    if meta.get("format-version") not in (1, 2):
        raise ValueError(f"format-version lạ: {meta.get('format-version')}")
    return meta


def summarize(meta: dict) -> list[dict]:
    """Một dict mỗi snapshot, theo thứ tự thời gian, các số đếm đã ép sang int."""
    current = meta.get("current-snapshot-id")
    rows = []
    for s in sorted(meta.get("snapshots", []), key=lambda s: s["timestamp-ms"]):
        summary = s.get("summary", {})
        rows.append(
            {
                "snapshot_id": s["snapshot-id"],
                "parent_id": s.get("parent-snapshot-id"),
                "committed_at": datetime.fromtimestamp(s["timestamp-ms"] / 1000, tz=UTC),
                "operation": summary.get("operation", "?"),
                "added_records": int(summary.get("added-records", 0)),
                "deleted_records": int(summary.get("deleted-records", 0)),
                "total_records": int(summary.get("total-records", 0)),
                "manifest_list": s.get("manifest-list", ""),
                "is_current": s["snapshot-id"] == current,
            }
        )
    return rows


def render(meta: dict, rows: list[dict]) -> str:
    lines = [
        f"table-uuid        : {meta.get('table-uuid')}",
        f"format-version    : {meta.get('format-version')}",
        f"location          : {meta.get('location')}",
        f"current-snapshot  : {meta.get('current-snapshot-id')}",
        f"schemas           : {len(meta.get('schemas', []))} (current-schema-id={meta.get('current-schema-id')})",
        f"partition-specs   : {len(meta.get('partition-specs', []))}",
        "",
        f"{'snapshot_id':>20}  {'committed_at (UTC)':<20} {'operation':<10} {'added':>6} {'deleted':>7} {'total':>6}",
    ]
    for r in rows:
        mark = "*" if r["is_current"] else " "
        lines.append(
            f"{mark}{r['snapshot_id']:>19}  {r['committed_at']:%Y-%m-%d %H:%M:%S}  "
            f"{r['operation']:<10} {r['added_records']:>6} {r['deleted_records']:>7} {r['total_records']:>6}"
        )
    lines.append("")
    lines.append(
        "* = snapshot hiện hành. VERSION AS OF <snapshot_id> đọc đúng manifest-list của dòng đó."
    )
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__)
        return 2
    meta_dir = Path(argv[1])
    path = find_current_metadata(meta_dir)
    meta = load_metadata(path)
    print(f"metadata file     : {path.name}")
    print(render(meta, summarize(meta)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
