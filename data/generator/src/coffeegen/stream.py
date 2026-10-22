"""Kafka producer for the streaming part (video #13+).

    coffeegen stream --topic pos.transactions --bootstrap localhost:9092 --rate 5
    coffeegen stream --source iot --dry-run        # prints JSON lines, no Kafka needed

Messages are keyed by store_id (POS/app) or machine_id (IoT) so a partition keeps order per key.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date

from .app_events import simulate_app_day
from .config import build_stores
from .dirty import dirty_pos
from .export import txn_event
from .iot import simulate_iot
from .pos import simulate_day


def _messages(source: str, day: date, stores_n: int, seed: int, scale: float):
    stores = build_stores(stores_n)
    if source == "pos":
        txns = []
        for s in stores:
            txns.extend(simulate_day(s, day, seed, scale))
        txns.sort(key=lambda t: t.ts)
        for t, kind in dirty_pos(txns, seed):
            yield t.store_id or "", txn_event(t, kind)
    elif source == "app-events":
        for e in simulate_app_day(stores, day, seed, scale):
            yield e["store_id"] or "", e
    elif source == "iot":
        for r in simulate_iot(stores, 2 * stores_n, day, hours=16, hz=0.2, seed=seed):
            yield r["machine_id"], r
    else:
        raise SystemExit(f"unknown source {source!r}")


def run_stream(
    topic: str,
    bootstrap: str,
    source: str,
    rate: float,
    day: date,
    stores_n: int,
    seed: int,
    scale: float,
    dry_run: bool,
    limit: int | None,
) -> int:
    producer = None
    if not dry_run:
        try:
            from confluent_kafka import Producer
        except ImportError as exc:  # pragma: no cover
            raise SystemExit(
                "confluent-kafka chưa cài. Chạy: uv pip install 'coffeegen[kafka]' hoặc dùng --dry-run"
            ) from exc
        producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 50})

    sent = 0
    delay = 1.0 / rate if rate > 0 else 0
    for key, msg in _messages(source, day, stores_n, seed, scale):
        payload = json.dumps(msg, ensure_ascii=False)
        if producer is None:
            sys.stdout.write(f"{key}\t{payload}\n")
        else:
            producer.produce(topic, key=key.encode(), value=payload.encode())
            producer.poll(0)
        sent += 1
        if limit and sent >= limit:
            break
        if delay:
            time.sleep(delay)
    if producer is not None:
        producer.flush()
    print(f"sent {sent} messages to {topic if producer else 'stdout'}", file=sys.stderr)
    return sent
