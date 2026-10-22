"""Lời giải lab #14 (2) — Dead Letter Queue cho pos.transactions.

    uv run --with confluent-kafka python videos/14-delivery-semantics-dlq-schema-registry/lab/solution/dlq_consumer.py \
        --group lab14-dlq --from-beginning
"""

from __future__ import annotations

import argparse
import json
from collections import Counter

from confluent_kafka import Consumer, KafkaException, Message, Producer


def validate(raw: bytes) -> tuple[dict | None, str | None]:
    """Trả (event, None) nếu hợp lệ, (None, reason) nếu không. Thuần Python để test được."""
    try:
        event = json.loads(raw)
    except (ValueError, UnicodeDecodeError):
        return None, "invalid_json"
    if not isinstance(event, dict):
        return None, "invalid_json"
    if event.get("store_id") is None:
        return None, "null_store"
    if int(event.get("total_amount_vnd", 0)) < 0:
        return None, "negative_amount"
    if not event.get("items"):
        return None, "no_items"
    return event, None


def send_to_dlq(producer: Producer, dlq_topic: str, msg: Message, reason: str) -> None:
    """Gửi nguyên message gốc sang DLQ; lý do và nguồn gốc nằm trong header, không sửa payload."""
    producer.produce(
        dlq_topic,
        key=msg.key(),
        value=msg.value(),
        headers=[
            ("reason", reason.encode()),
            ("source_topic", msg.topic().encode()),
            ("source_partition", str(msg.partition()).encode()),
            ("source_offset", str(msg.offset()).encode()),
        ],
    )
    producer.poll(0)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="pos.transactions")
    p.add_argument("--dlq-topic", default="pos.transactions.dlq")
    p.add_argument("--group", default="lab14-dlq")
    p.add_argument("--from-beginning", action="store_true")
    args = p.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap,
            "group.id": args.group,
            "auto.offset.reset": "earliest" if args.from_beginning else "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])
    producer = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True})

    stats: Counter[str] = Counter()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            event, reason = validate(msg.value())
            if event is None:
                stats[reason or "unknown"] += 1
                send_to_dlq(producer, args.dlq_topic, msg, reason or "unknown")
                print(f"→ DLQ  reason={reason}  p{msg.partition()}@{msg.offset()}")
            else:
                stats["ok"] += 1

            producer.flush()  # DLQ đã ghi chắc chắn...
            consumer.commit(asynchronous=False)  # ...rồi mới commit offset nguồn
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        consumer.close()
        print("\n", dict(stats))


if __name__ == "__main__":
    main()
