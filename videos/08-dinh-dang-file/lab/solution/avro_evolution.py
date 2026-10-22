"""Lời giải lab #08 (phần Avro) — CHỈ CHẠY TRONG JUPYTER (image có fastavro; .venv trên máy KHÔNG có).

Trong Jupyter (http://localhost:8888, token dea2z):
    !python work/videos/08-dinh-dang-file/lab/solution/avro_evolution.py --out /tmp/avro

Minh hoạ schema evolution: writer schema v1 không có `discount_vnd`; reader schema v2 thêm cột với default.
Avro đọc file cũ bằng schema mới mà không lỗi — đó là lý do Kafka + Schema Registry (video #14) chọn Avro.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
SAMPLES = REPO / "data" / "samples"

SCHEMA_V1 = {
    "type": "record",
    "name": "PosTransaction",
    "namespace": "vn.phinpho.pos",
    "fields": [
        {"name": "transaction_id", "type": "string"},
        {"name": "store_id", "type": ["null", "string"], "default": None},
        {"name": "ts", "type": "string"},
        {"name": "channel", "type": "string"},
        {"name": "total_amount_vnd", "type": "long"},
    ],
}

# v2 = v1 + discount_vnd. Thêm field MỚI phải có default thì reader v2 mới đọc được file v1.
SCHEMA_V2 = {
    **SCHEMA_V1,
    "fields": [*SCHEMA_V1["fields"], {"name": "discount_vnd", "type": "long", "default": 0}],
}


def rows_v1(limit: int) -> list[dict]:
    with (SAMPLES / "pos_transactions.csv").open(encoding="utf8") as f:
        out = []
        for r in csv.DictReader(f):
            out.append(
                {
                    "transaction_id": r["transaction_id"],
                    "store_id": r["store_id"] or None,
                    "ts": r["ts"],
                    "channel": r["channel"],
                    "total_amount_vnd": int(r["total_amount_vnd"]),
                }
            )
            if len(out) >= limit:
                return out
    return out


def main() -> None:
    import fastavro  # chỉ có trong Jupyter image

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default="/tmp/avro")
    p.add_argument("--limit", type=int, default=500)
    a = p.parse_args()
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    path = out / "pos_transactions.v1.avro"

    parsed_v1 = fastavro.parse_schema(SCHEMA_V1)
    with path.open("wb") as f:
        fastavro.writer(f, parsed_v1, rows_v1(a.limit), codec="deflate")
    print(f"wrote {path} ({path.stat().st_size:,} bytes) with schema v1")

    # Đọc file v1 bằng reader schema v2: fastavro tự điền default cho discount_vnd.
    parsed_v2 = fastavro.parse_schema(SCHEMA_V2)
    with path.open("rb") as f:
        reader = fastavro.reader(f, reader_schema=parsed_v2)
        first = next(reader)
        n = 1 + sum(1 for _ in reader)
    print(f"read back {n} records with schema v2; first record: {first}")
    assert first["discount_vnd"] == 0, "default của field mới phải được điền"

    # Thử xoá default → reader v2 KHÔNG tương thích → fastavro báo SchemaResolutionError.
    bad_v2 = {
        **SCHEMA_V1,
        "fields": [*SCHEMA_V1["fields"], {"name": "discount_vnd", "type": "long"}],
    }
    try:
        with path.open("rb") as f:
            next(fastavro.reader(f, reader_schema=fastavro.parse_schema(bad_v2)))
    except Exception as e:  # noqa: BLE001 — demo: tên exception khác nhau giữa các bản fastavro
        print(
            f"reader without default → {type(e).__name__}: schema KHÔNG tương thích ngược, đúng như mong đợi"
        )


if __name__ == "__main__":
    main()
