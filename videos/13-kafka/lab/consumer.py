"""Lab #13 — consumer đọc topic pos.transactions, in doanh thu luỹ kế theo cửa hàng.

Chạy từ máy bạn (uv tải confluent-kafka vào môi trường tạm, không sửa pyproject):
    uv run --with confluent-kafka python videos/13-kafka/lab/consumer.py --group lab13 --from-beginning
Chạy trong terminal của Jupyter (đã có confluent-kafka):
    python work/videos/13-kafka/lab/consumer.py --bootstrap kafka:29092 --group lab13 --from-beginning

Ctrl+C để dừng: consumer commit offset rồi thoát, lần sau chạy tiếp từ chỗ dừng.
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
    """Kafka gọi hàm này mỗi lần chia lại partition (rebalance) — in ra để thấy ở bước 7."""
    print("assigned:", [f"{p.topic}[{p.partition}]" for p in partitions])


def on_revoke(consumer: Consumer, partitions: list[TopicPartition]) -> None:
    print("revoked:", [f"{p.topic}[{p.partition}]" for p in partitions])


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="pos.transactions")
    p.add_argument("--group", default="lab13")
    p.add_argument(
        "--from-beginning", action="store_true", help="đọc từ offset đầu nếu group chưa có offset"
    )
    args = p.parse_args()

    conf = {
        "bootstrap.servers": args.bootstrap,
        # TODO 1: cấu hình consumer group.
        #   "group.id": ...                 — tên group (args.group); cùng group → chia partition
        #   "auto.offset.reset": ...        — "earliest" nếu args.from_beginning, ngược lại "latest"
        #   "enable.auto.commit": False     — mình tự commit để kiểm soát (video #14 giải thích vì sao)
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

            # TODO 2: parse value (bytes JSON) → dict; store = event["store_id"] or "(null)";
            #   cộng total_amount_vnd vào revenue[store], tăng orders[store].
            store = "(todo)"

            # TODO 3: in một dòng: p{partition}@{offset}  {store}  doanh thu luỹ kế của store đó
            #   gợi ý: msg.partition(), msg.offset(), fmt_vnd(revenue[store])

            processed += 1
            # TODO 4: mỗi COMMIT_EVERY message gọi consumer.commit(asynchronous=False).
            #   Thử bỏ commit và chạy lại chương trình: nó đọc lại từ đâu?
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
