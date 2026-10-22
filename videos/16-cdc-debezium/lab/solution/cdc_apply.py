"""Lời giải lab #16 — áp dụng CDC envelope của Debezium vào bảng DuckDB local (upsert / delete).

    uv run --with confluent-kafka python videos/16-cdc-debezium/lab/solution/cdc_apply.py --db /tmp/cdc_pos.duckdb
    uv run python -c "import duckdb; print(duckdb.connect('/tmp/cdc_pos.duckdb').sql('SELECT _op, count(*) FROM pos_transactions GROUP BY 1'))"

Envelope (value.converter.schemas.enable=false):
    {"before": {...}|null, "after": {...}|null, "source": {...}, "op": "c"|"u"|"d"|"r", "ts_ms": 1739...}
`r` = dòng từ snapshot ban đầu. Sau `d` Debezium gửi thêm tombstone (value null) để Kafka compaction xoá key.
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
    ts               VARCHAR,      -- Debezium gửi timestamptz dạng chuỗi ISO (UTC); ép kiểu ở silver
    channel          VARCHAR,
    payment_method   VARCHAR,
    customer_id      VARCHAR,
    staff_id         VARCHAR,
    total_amount_vnd BIGINT,
    discount_vnd     BIGINT,
    is_rainy_day     BOOLEAN,
    _op              VARCHAR,      -- op cuối cùng đã áp dụng (r/c/u)
    _ts_ms           BIGINT        -- ts_ms của envelope đó
)
"""


def init_db(path: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path)
    con.execute(DDL)
    return con


def unwrap(value: bytes | str | None) -> dict | None:
    """bytes JSON → envelope dict. Tombstone (value null) → None. Chịu được cả dạng có bọc schema/payload."""
    if value is None:
        return None
    env = json.loads(value)
    if "payload" in env:  # value.converter.schemas.enable=true
        env = env["payload"]
    return env


def apply_envelope(con: duckdb.DuckDBPyConnection, env: dict) -> str:
    """Áp dụng một envelope; trả về op. Idempotent: áp dụng lại cùng envelope không đổi trạng thái."""
    op = env["op"]
    if op in ("c", "u", "r"):
        row = env["after"]
        values = [row.get(c) for c in COLUMNS] + [op, env.get("ts_ms")]
        placeholders = ", ".join("?" for _ in values)
        con.execute(
            f"INSERT OR REPLACE INTO pos_transactions ({', '.join(COLUMNS)}, _op, _ts_ms) VALUES ({placeholders})",
            values,
        )
    elif op == "d":
        con.execute(
            "DELETE FROM pos_transactions WHERE transaction_id = ?",
            [env["before"]["transaction_id"]],
        )
    else:  # "t" (truncate), "m" (message) — ngoài phạm vi lab
        pass
    return op


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
            # commit sau khi ghi DuckDB: at-least-once, nhưng apply_envelope idempotent nên đọc lại không sao
            consumer.commit(asynchronous=False)
            if sum(stats.values()) % 500 == 0:
                print(dict(stats))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        n = con.execute("SELECT count(*) FROM pos_transactions").fetchone()[0]
        print(f"\nenvelopes: {dict(stats)}  →  {n} dòng trong {args.db}")


if __name__ == "__main__":
    main()
