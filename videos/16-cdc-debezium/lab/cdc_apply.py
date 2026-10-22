"""Lab #16 — consumer áp dụng CDC envelope của Debezium vào bảng DuckDB local (upsert / delete).

    uv run --with confluent-kafka python videos/16-cdc-debezium/lab/cdc_apply.py --db /tmp/cdc_pos.duckdb

Envelope (value.converter.schemas.enable=false trong register-connector.json):
    {"before": {...}|null, "after": {...}|null, "source": {...}, "op": "c"|"u"|"d"|"r", "ts_ms": 1739...}
`r` = dòng từ snapshot ban đầu. Sau `d` Debezium gửi thêm một tombstone (value = null) cùng key.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter

import duckdb

COLUMNS = [
    "transaction_id",
    "store_id",
    "ts",
    "channel",
    "payment_method",
    "customer_id",
    "staff_id",
    "total_amount_vnd",
    "discount_vnd",
    "is_rainy_day",
]
DDL = """
CREATE TABLE IF NOT EXISTS pos_transactions (
    transaction_id   VARCHAR PRIMARY KEY,
    store_id         VARCHAR,
    ts               VARCHAR,
    channel          VARCHAR,
    payment_method   VARCHAR,
    customer_id      VARCHAR,
    staff_id         VARCHAR,
    total_amount_vnd BIGINT,
    discount_vnd     BIGINT,
    is_rainy_day     BOOLEAN,
    _op              VARCHAR,
    _ts_ms           BIGINT
)
"""


def init_db(path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path)
    con.execute(DDL)
    return con


def unwrap(value: bytes | str | None) -> dict | None:
    """bytes JSON → envelope dict; tombstone (None) → None.

    TODO 1: json.loads; nếu dict có key "payload" (schemas.enable=true) thì lấy phần payload.
    """
    raise NotImplementedError


def apply_envelope(con: duckdb.DuckDBPyConnection, env: dict) -> str:
    """Áp dụng một envelope vào bảng, trả về op.

    TODO 2: op in ("c", "u", "r") → lấy env["after"], INSERT OR REPLACE theo PRIMARY KEY transaction_id
            (DuckDB: `INSERT OR REPLACE INTO pos_transactions (...) VALUES (?, ...)`), ghi thêm _op, _ts_ms.
    TODO 3: op == "d" → DELETE theo env["before"]["transaction_id"] (nhớ: after là null khi delete).
    Hàm phải idempotent: áp dụng lại cùng envelope không làm đổi trạng thái — vì sao điều này quan trọng
    với consumer at-least-once?
    """
    raise NotImplementedError


def main() -> None:
    from confluent_kafka import Consumer, KafkaException

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="coffee.public.pos_transactions")
    p.add_argument("--group", default="lab16-cdc-apply")
    p.add_argument("--db", default="cdc_pos.duckdb")
    args = p.parse_args()

    con = init_db(args.db)
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])

    stats: Counter[str] = Counter()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            env = unwrap(msg.value())
            if env is None:
                stats["tombstone"] += 1
            else:
                op = apply_envelope(con, env)
                stats[op] += 1
                if op != "r":
                    print(f"{op}  {msg.key()}  ts_ms={env.get('ts_ms')}")
            consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        n = con.execute("SELECT count(*) FROM pos_transactions").fetchone()[0]
        print(f"\nenvelopes: {dict(stats)}  →  {n} dòng trong {args.db}")


if __name__ == "__main__":
    main()
