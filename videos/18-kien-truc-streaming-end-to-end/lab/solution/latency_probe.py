"""Lời giải lab #18 — đo độ trễ end-to-end: Kafka (lúc message vào broker) → file Parquet ở gold.

Chạy trong container Jupyter (có confluent-kafka, boto3, duckdb):
    docker compose exec jupyter python /home/jovyan/work/videos/18-kien-truc-streaming-end-to-end/lab/solution/latency_probe.py \
        --bootstrap kafka:29092 --s3-endpoint http://minio:9000 --duration 240
Từ host:
    uv run --with confluent-kafka --with boto3 python videos/18-kien-truc-streaming-end-to-end/lab/solution/latency_probe.py --duration 240

Cách đo: một luồng "tap" đọc pos.transactions và ghi, cho mỗi (window_start, store_id), timestamp Kafka lớn nhất
(= lúc message cuối của cửa sổ vào broker — message POS không có ingested_at nên dùng timestamp của broker).
Luồng chính liệt kê object mới ở s3://lake/gold/rt_store_revenue_1m/, đọc (window.start, store_id) bằng DuckDB,
latency = LastModified(object) − tap[(window_start, store_id)].
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import threading
import time
from datetime import UTC, datetime

import duckdb

WINDOW_S = 60  # cửa sổ 1 phút của streaming_pos_agg.py


def window_start_ms(ts: str, window_s: int = WINDOW_S) -> int:
    """Đầu cửa sổ chứa `ts`, epoch ms, UTC. Thiếu offset → hiểu là UTC (đúng như Spark với session tz UTC)."""
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    epoch_ms = int(dt.timestamp() * 1000)
    return epoch_ms - epoch_ms % (window_s * 1000)


def percentile(values: list[float], q: float) -> float:
    """Percentile kiểu nearest-rank; đủ cho vài trăm điểm đo."""
    if not values:
        return float("nan")
    s = sorted(values)
    k = max(0, min(len(s) - 1, round(q * (len(s) - 1))))
    return s[k]


def tap_kafka(
    bootstrap: str, topic: str, tap: dict[tuple[int, str], int], stop: threading.Event
) -> None:
    """Chạy trong thread: ghi timestamp Kafka lớn nhất cho mỗi (window_start_ms, store_id)."""
    from confluent_kafka import Consumer

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"latency-probe-{int(time.time())}",  # group mới mỗi lần → không đụng offset của ai
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    try:
        while not stop.is_set():
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            event = json.loads(msg.value())
            if not event.get("store_id"):
                continue  # job Spark cũng bỏ store_id null
            _, ts_ms = msg.timestamp()  # (TIMESTAMP_CREATE_TIME | LOG_APPEND_TIME, epoch ms)
            key = (window_start_ms(event["ts"]), event["store_id"])
            tap[key] = max(tap.get(key, 0), ts_ms)
    finally:
        consumer.close()


def read_windows(parquet_path: str) -> list[tuple[int, str]]:
    """(window_start epoch ms, store_id) của mọi dòng trong một file Parquet của sink."""
    rows = duckdb.sql(
        f"SELECT epoch_ms(window.start) AS w, store_id FROM read_parquet('{parquet_path}')"
    ).fetchall()
    return [(int(w), s) for w, s in rows]


def main() -> None:
    import boto3

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="pos.transactions")
    p.add_argument("--s3-endpoint", default="http://localhost:9000")
    p.add_argument("--bucket", default="lake")
    p.add_argument("--prefix", default="gold/rt_store_revenue_1m/")
    p.add_argument("--interval", type=float, default=5.0, help="giây giữa 2 lần liệt kê S3")
    p.add_argument("--duration", type=float, default=300.0, help="tổng thời gian đo (giây)")
    args = p.parse_args()

    s3 = boto3.client(
        "s3",
        endpoint_url=args.s3_endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name="us-east-1",
    )

    tap: dict[tuple[int, str], int] = {}
    stop = threading.Event()
    threading.Thread(
        target=tap_kafka, args=(args.bootstrap, args.topic, tap, stop), daemon=True
    ).start()

    # file đã có trước khi đo thì bỏ qua — chỉ đo file mới
    seen = {
        o["Key"]
        for o in s3.list_objects_v2(Bucket=args.bucket, Prefix=args.prefix).get("Contents", [])
    }
    print(
        f"tap {args.topic} @ {args.bootstrap}; theo dõi s3://{args.bucket}/{args.prefix} ({len(seen)} file cũ bỏ qua)"
    )

    latencies_ms: list[float] = []
    t_end = time.time() + args.duration
    with tempfile.TemporaryDirectory() as tmp:
        while time.time() < t_end:
            time.sleep(args.interval)
            objs = s3.list_objects_v2(Bucket=args.bucket, Prefix=args.prefix).get("Contents", [])
            for o in sorted(objs, key=lambda o: o["LastModified"]):
                key = o["Key"]
                if key in seen or not key.endswith(".parquet"):
                    continue
                seen.add(key)
                local = os.path.join(tmp, os.path.basename(key))
                s3.download_file(args.bucket, key, local)
                sink_ms = int(o["LastModified"].timestamp() * 1000)
                windows = read_windows(local)
                matched = [sink_ms - tap[w] for w in windows if w in tap]
                latencies_ms.extend(matched)
                print(
                    f"{o['LastModified']:%H:%M:%S}  {os.path.basename(key)[:40]:<40} "
                    f"windows={len(windows):>3} matched={len(matched):>3} "
                    f"p50={percentile(matched, 0.5) / 1000:6.1f}s p95={percentile(matched, 0.95) / 1000:6.1f}s"
                )
    stop.set()

    print(f"\n{len(latencies_ms)} cửa sổ đo được")
    if latencies_ms:
        print(
            f"end-to-end latency  p50={percentile(latencies_ms, 0.5) / 1000:.1f}s  "
            f"p95={percentile(latencies_ms, 0.95) / 1000:.1f}s  max={max(latencies_ms) / 1000:.1f}s"
        )
        print(
            "Phần lớn con số này là watermark (10 phút event-time) + micro-batch, không phải Kafka hay Spark 'chậm'."
        )


if __name__ == "__main__":
    main()
