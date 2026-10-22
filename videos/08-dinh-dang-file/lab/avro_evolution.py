"""Lab #08 (phần Avro) — CHỈ CHẠY TRONG JUPYTER (image có fastavro; .venv trên máy KHÔNG có).

Trong Jupyter (http://localhost:8888, token dea2z):
    !python work/videos/08-dinh-dang-file/lab/avro_evolution.py --out /tmp/avro

Mục tiêu: ghi file Avro bằng schema v1, rồi đọc lại bằng schema v2 (thêm 1 field) mà không lỗi.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
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

# TODO 1: viết SCHEMA_V2 = SCHEMA_V1 + field "discount_vnd" kiểu long.
#   Câu hỏi: field mới cần thuộc tính gì để reader v2 đọc được file v1? (gợi ý: "default")
SCHEMA_V2 = SCHEMA_V1


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

    # TODO 2: mở lại file, đọc bằng reader_schema=fastavro.parse_schema(SCHEMA_V2),
    #   in record đầu tiên và kiểm tra first["discount_vnd"] == 0.
    #   gợi ý: fastavro.reader(f, reader_schema=...)

    # TODO 3: thử bỏ "default" khỏi field mới và đọc lại → bạn gặp exception gì? Vì sao?


if __name__ == "__main__":
    main()
