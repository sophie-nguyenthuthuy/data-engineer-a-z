"""Lab #17 — đọc thư mục metadata/ của một bảng Iceberg (hadoop catalog) bằng stdlib, in danh sách snapshots.

    uv run python videos/17-lakehouse-iceberg-delta-hudi/lab/metadata_explorer.py <thư mục metadata/>
Thử ngay với fixture (không cần Docker):
    uv run python videos/17-lakehouse-iceberg-delta-hudi/lab/metadata_explorer.py \
        videos/17-lakehouse-iceberg-delta-hudi/lab/solution/fixtures/metadata
Với bảng thật trên MinIO:
    docker compose exec minio mc cp -r local/warehouse/silver/pos_transactions/metadata /tmp/meta
    docker compose cp minio:/tmp/meta ./tmp-meta && uv run python .../metadata_explorer.py ./tmp-meta

Cấu trúc (Iceberg spec v2): version-hint.text → vN.metadata.json → snapshots[] → manifest-list (.avro) → manifests → data files.
Lab chỉ đọc JSON; .avro cần thư viện Avro.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path


def find_current_metadata(meta_dir: Path) -> Path:
    r"""Trả đường dẫn vN.metadata.json hiện hành.

    TODO 1: đọc meta_dir / "version-hint.text" → số N → meta_dir / f"v{N}.metadata.json".
            Nếu không có hint (JDBC/REST catalog): file tên vN.metadata.json hoặc NNNNN-<uuid>.metadata.json
            → chọn file có số thứ tự đầu tên lớn nhất (glob + regex r"v?(\d+)").
    """
    raise NotImplementedError


def load_metadata(path: Path) -> dict:
    """json.loads; kiểm tra "format-version" là 1 hoặc 2 (TODO 2)."""
    raise NotImplementedError


def summarize(meta: dict) -> list[dict]:
    """Một dict mỗi snapshot, sắp theo timestamp-ms tăng dần.

    TODO 3: với mỗi s trong meta["snapshots"] lấy:
        snapshot_id = s["snapshot-id"], parent_id = s.get("parent-snapshot-id"),
        committed_at = datetime.fromtimestamp(s["timestamp-ms"] / 1000, tz=UTC),
        operation = s["summary"]["operation"], added_records / deleted_records / total_records (int, mặc định 0,
        key trong summary là "added-records", "deleted-records", "total-records"),
        manifest_list = s["manifest-list"], is_current = (snapshot_id == meta["current-snapshot-id"]).
    """
    raise NotImplementedError


def render(meta: dict, rows: list[dict]) -> str:
    """TODO 4: in table-uuid, format-version, current-snapshot-id rồi một dòng mỗi snapshot; đánh * dòng hiện hành."""
    raise NotImplementedError


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__)
        return 2
    path = find_current_metadata(Path(argv[1]))
    meta = load_metadata(path)
    print(f"metadata file     : {path.name}")
    print(render(meta, summarize(meta)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
