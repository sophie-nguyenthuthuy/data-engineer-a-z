"""Lab #14 (2) — Dead Letter Queue: message hỏng → topic pos.transactions.dlq kèm header lý do.

    uv run --with confluent-kafka python videos/14-delivery-semantics-dlq-schema-registry/lab/dlq_consumer.py \
        --group lab14-dlq --from-beginning
"""

from __future__ import annotations

import argparse
import json
from collections import Counter

from confluent_kafka import Consumer, KafkaException, Message, Producer


def validate(raw: bytes) -> tuple[dict | None, str | None]:
    """Trả (event, None) nếu hợp lệ, (None, reason) nếu không.

    TODO 1: cài đặt 4 luật, theo thứ tự:
      - json.loads lỗi                      → "invalid_json"
      - event.get("store_id") is None       → "null_store"
      - total_amount_vnd < 0                → "negative_amount"
      - items rỗng hoặc thiếu               → "no_items"
    """
    raise NotImplementedError


def send_to_dlq(producer: Producer, dlq_topic: str, msg: Message, reason: str) -> None:
    """Gửi NGUYÊN message gốc (key + value) sang DLQ, thêm header giải thích.

    TODO 2: producer.produce(dlq_topic, key=msg.key(), value=msg.value(), headers=[...])
      headers là list[tuple[str, bytes]]: reason, source_topic, source_partition, source_offset.
      Gợi ý: msg.topic(), str(msg.partition()).encode(), str(msg.offset()).encode()
    """
    raise NotImplementedError


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
    producer = Producer({"bootstrap.servers": args.bootstrap})

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

            # TODO 3: thứ tự đúng là producer.flush() (DLQ đã ghi xong) RỒI mới consumer.commit().
            #   Nếu commit trước mà flush fail thì message hỏng biến mất — đó là at-most-once cho DLQ.
            consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        consumer.close()
        print("\n", dict(stats))


if __name__ == "__main__":
    main()
