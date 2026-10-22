"""Lab #14 (1) — consumer idempotent: bỏ qua hoá đơn đã thấy (duplicate do producer retry).

    uv run --with confluent-kafka python videos/14-delivery-semantics-dlq-schema-registry/lab/idempotent_consumer.py \
        --group lab14-idem --from-beginning
"""

from __future__ import annotations

import argparse
import json

from confluent_kafka import Consumer, KafkaException


def fmt_vnd(n: int) -> str:
    return f"{n:,}".replace(",", ".") + " VND"


def is_duplicate(transaction_id: str, seen: set[str]) -> bool:
    """True nếu đã xử lý transaction_id này; ngược lại ghi nhớ và trả False.

    TODO 1: cài đặt. Gợi ý: `in` rồi `add`. Hàm này thuần Python để bạn test được không cần Kafka.
    """
    raise NotImplementedError


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="pos.transactions")
    p.add_argument("--group", default="lab14-idem")
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

    seen: set[str] = set()
    processed = duplicates = 0
    revenue_raw = revenue_dedup = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            event = json.loads(msg.value())
            processed += 1
            revenue_raw += int(event["total_amount_vnd"])

            # TODO 2: nếu is_duplicate(event["transaction_id"], seen) → tăng duplicates,
            #   in "dup <transaction_id>", KHÔNG cộng doanh thu; ngược lại cộng vào revenue_dedup.

            # at-least-once: commit SAU khi xử lý
            consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print(f"\nprocessed={processed} duplicates={duplicates} distinct={len(seen)}")
        print(f"doanh thu không dedup: {fmt_vnd(revenue_raw)}")
        print(f"doanh thu dedup      : {fmt_vnd(revenue_dedup)}")


if __name__ == "__main__":
    main()
