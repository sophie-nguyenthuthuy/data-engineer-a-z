"""Lab #18 — đo độ trễ end-to-end: Kafka (lúc message vào broker) → file Parquet ở gold.

Chạy trong container Jupyter (có confluent-kafka, boto3, duckdb):
    docker compose exec jupyter python /home/jovyan/work/videos/18-kien-truc-streaming-end-to-end/lab/latency_probe.py \
        --bootstrap kafka:29092 --s3-endpoint http://minio:9000 --duration 240
Từ host:
    uv run --with confluent-kafka --with boto3 python videos/18-kien-truc-streaming-end-to-end/lab/latency_probe.py --duration 240

Ý tưởng: message POS không có ingested_at, nhưng Kafka gắn timestamp lúc message vào broker (msg.timestamp()).
Sink Parquet có LastModified của từng object trên MinIO. Latency của một cửa sổ (window.start, store_id)
= LastModified(file chứa cửa sổ) − max(timestamp Kafka của các message thuộc cửa sổ đó).
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

WINDOW_S = 60  # cửa sổ 1 phút của platform/spark/streaming_pos_agg.py


def window_start_ms(ts: str, window_s: int = WINDOW_S) -> int:
    """Đầu cửa sổ chứa `ts` (chuỗi ISO từ message), epoch ms UTC.

    TODO 1: datetime.fromisoformat(ts); nếu không có tzinfo → coi là UTC (giống Spark với session tz UTC —
            và đó là lý do dòng tz_drift rơi vào cửa sổ sai); epoch ms rồi làm tròn xuống bội của window_s*1000.
    """
    raise NotImplementedError


def percentile(values: list[float], q: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = max(0, min(len(s) - 1, round(q * (len(s) - 1))))
    return s[k]


def tap_kafka(
    bootstrap: str, topic: str, tap: dict[tuple[int, str], int], stop: threading.Event
) -> None:
    """Chạy trong thread: với mỗi message, tap[(window_start_ms, store_id)] = max(timestamp Kafka).

    TODO 2: Consumer với group.id mới mỗi lần chạy (f"latency-probe-{int(time.time())}"), auto.offset.reset latest;
            vòng while not stop.is_set(): poll(0.5); bỏ message lỗi / store_id null;
            _, ts_ms = msg.timestamp(); cập nhật tap. Nhớ consumer.close() trong finally.
    """
    raise NotImplementedError


def read_windows(parquet_path: str) -> list[tuple[int, str]]:
    """(window_start epoch ms, store_id) của mọi dòng trong một file Parquet của sink.

    TODO 3: duckdb.sql("SELECT epoch_ms(window.start), store_id FROM read_parquet(...)").fetchall()
    """
    raise NotImplementedError


def main() -> None:
    import boto3

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="pos.transactions")
    p.add_argument("--s3-endpoint", default="http://localhost:9000")
    p.add_argument("--bucket", default="lake")
    p.add_argument("--prefix", default="gold/rt_store_revenue_1m/")
    p.add_argument("--interval", type=float, default=5.0)
    p.add_argument("--duration", type=float, default=300.0)
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

    seen = {
        o["Key"]
        for o in s3.list_objects_v2(Bucket=args.bucket, Prefix=args.prefix).get("Contents", [])
    }
    print(
        f"tap {args.topic}; theo dõi s3://{args.bucket}/{args.prefix} ({len(seen)} file cũ bỏ qua)"
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
                    f"p50={percentile(matched, 0.5) / 1000:6.1f}s"
                )
    stop.set()

    print(f"\n{len(latencies_ms)} cửa sổ đo được")
    if latencies_ms:
        print(
            f"p50={percentile(latencies_ms, 0.5) / 1000:.1f}s  p95={percentile(latencies_ms, 0.95) / 1000:.1f}s"
        )


if __name__ == "__main__":
    main()
