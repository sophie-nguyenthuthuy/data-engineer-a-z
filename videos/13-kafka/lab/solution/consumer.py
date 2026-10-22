"""Lời giải lab #13 — consumer tính doanh thu luỹ kế theo cửa hàng.

uv run --with confluent-kafka python videos/13-kafka/lab/solution/consumer.py --group lab13 --from-beginning
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

COMMIT_EVERY = 50  # message


def fmt_vnd(n: int) -> str:
    return f"{n:,}".replace(",", ".") + " VND"


def on_assign(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    print("assigned:", [f"{p.topic}[{p.partition}]" for p in partitions])


def on_revoke(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    print("revoked:", [f"{p.topic}[{p.partition}]" for p in partitions])


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="pos.transactions")
    p.add_argument("--group", default="lab13")
    p.add_argument("--from-beginning", action="store_true")
    args = p.parse_args()

    conf = {
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        # chỉ có tác dụng khi group CHƯA có offset đã commit cho partition đó
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke)

    revenue: dict[str, int] = defaultdict(int)
    orders: dict[str, int] = defaultdict(int)
    processed = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            event = json.loads(msg.value())
            store = event["store_id"] or "(null)"
            revenue[store] += int(event["total_amount_vnd"])
            orders[store] += 1
            print(
                f"p{msg.partition()}@{msg.offset():<5} {store:<7} "
                f"{orders[store]:>4} hoá đơn  luỹ kế {fmt_vnd(revenue[store])}"
            )

            processed += 1
            if processed % COMMIT_EVERY == 0:
                # at-least-once: xử lý xong rồi mới commit → crash giữa chừng sẽ đọc lại tối đa 50 message
                consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        if processed:
            try:
                consumer.commit(asynchronous=False)
            except KafkaException as exc:
                # _NO_OFFSET = không có offset mới kể từ lần commit trước (Ctrl+C đúng lúc processed % 50 == 0).
                # Không phải lỗi — nhưng librdkafka vẫn raise, và không bắt thì chương trình "crash" khi dừng.
                if exc.args[0].code() != KafkaError._NO_OFFSET:
                    raise
        consumer.close()
        total = sum(revenue.values())
        print(f"\n{processed} message, tổng {fmt_vnd(total)}")
        for store, amount in sorted(revenue.items(), key=lambda kv: -kv[1])[:5]:
            print(f"  {store:<7} {orders[store]:>4} hoá đơn  {fmt_vnd(amount)}")


if __name__ == "__main__":
    main()
