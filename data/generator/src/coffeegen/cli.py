"""Command-line interface. `coffeegen --help`."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from .app_events import simulate_app_day
from .config import MENU, build_stores
from .dirty import dirty_pos
from .export import (
    CUSTOMER_FIELDS,
    LINE_FIELDS,
    MENU_FIELDS,
    STORE_FIELDS,
    TXN_FIELDS,
    line_rows,
    txn_row,
)
from .iot import IOT_FIELDS, machines, simulate_iot
from .pos import customers, simulate_range
from .writers import write_csv, write_jsonl

DEFAULT_START = "2026-01-01"


def _common(p: argparse.ArgumentParser) -> None:
    p.add_argument("--seed", type=int, default=42, help="RNG seed (default 42)")
    p.add_argument(
        "--start",
        type=date.fromisoformat,
        default=date.fromisoformat(DEFAULT_START),
        help="first day, YYYY-MM-DD",
    )
    p.add_argument("--stores", type=int, default=30, help="number of stores (default 30)")
    p.add_argument(
        "--scale", type=float, default=1.0, help="volume multiplier (0.1 = 10%% of traffic)"
    )


def cmd_pos(a: argparse.Namespace) -> int:
    out = Path(a.out)
    stores = build_stores(a.stores)
    write_csv(
        out / "stores.csv",
        [s.__dict__ | {"opened_date": s.opened_date.isoformat()} for s in stores],
        STORE_FIELDS,
    )
    write_csv(out / "menu_items.csv", [m.__dict__ for m in MENU], MENU_FIELDS)
    write_csv(out / "customers.csv", customers(a.seed), CUSTOMER_FIELDS)
    n_txn = n_line = 0
    first = True
    for _day, txns in simulate_range(stores, a.start, a.days, a.seed, a.scale):
        tagged = dirty_pos(txns, a.seed) if not a.clean else [(t, None) for t in txns]
        n_txn += write_csv(
            out / "pos_transactions.csv",
            (txn_row(t, k) for t, k in tagged),
            TXN_FIELDS,
            append=not first,
        )
        n_line += write_csv(
            out / "pos_line_items.csv",
            (row for t, k in tagged if k != "duplicate" for row in line_rows(t)),
            LINE_FIELDS,
            append=not first,
        )
        first = False
    print(
        f"wrote {len(stores)} stores, {len(MENU)} menu items, {n_txn} transactions, {n_line} line items → {out}"
    )
    return 0


def cmd_app_events(a: argparse.Namespace) -> int:
    out = Path(a.out)
    stores = build_stores(a.stores)
    n = 0
    first = True
    for offset in range(a.days):
        d = date.fromordinal(a.start.toordinal() + offset)
        n += write_jsonl(
            out / "app_events.jsonl", simulate_app_day(stores, d, a.seed, a.scale), append=not first
        )
        first = False
    print(f"wrote {n} app events → {out / 'app_events.jsonl'}")
    return 0


def cmd_iot(a: argparse.Namespace) -> int:
    out = Path(a.out)
    stores = build_stores(a.stores)
    write_csv(
        out / "machines.csv",
        machines(stores, a.machines),
        ["machine_id", "store_id", "model", "installed_date"],
    )
    n = write_csv(
        out / "iot_readings.csv",
        simulate_iot(stores, a.machines, a.start, a.hours, a.hz, a.seed),
        IOT_FIELDS,
    )
    print(f"wrote {a.machines} machines, {n} readings → {out}")
    return 0


def cmd_stream(a: argparse.Namespace) -> int:
    from .stream import run_stream

    run_stream(
        a.topic,
        a.bootstrap,
        a.source,
        a.rate,
        a.start,
        a.stores,
        a.seed,
        a.scale,
        a.dry_run,
        a.limit,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="coffeegen", description="Synthetic coffee-chain data (deterministic)."
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("pos", help="stores, menu, customers, POS transactions + line items (CSV)")
    _common(s)
    s.add_argument("--days", type=int, default=90)
    s.add_argument("--out", default="./out")
    s.add_argument("--clean", action="store_true", help="disable dirty-data injection")
    s.set_defaults(func=cmd_pos)

    s = sub.add_parser(
        "app-events",
        help="ordering-app clickstream (JSONL), consistent with `pos` for the same seed",
    )
    _common(s)
    s.add_argument("--days", type=int, default=90)
    s.add_argument("--out", default="./out")
    s.set_defaults(func=cmd_app_events)

    s = sub.add_parser("iot", help="espresso-machine telemetry (CSV)")
    _common(s)
    s.add_argument("--machines", type=int, default=60)
    s.add_argument(
        "--hz",
        type=float,
        default=0.2,
        help="readings per second per machine (default 0.2 = every 5s)",
    )
    s.add_argument(
        "--hours", type=float, default=16, help="hours of telemetry from 06:00 (default 16)"
    )
    s.add_argument("--out", default="./out")
    s.set_defaults(func=cmd_iot)

    s = sub.add_parser(
        "stream", help="Kafka producer (needs confluent-kafka) or --dry-run to stdout"
    )
    _common(s)
    s.add_argument("--topic", default="pos.transactions")
    s.add_argument("--bootstrap", default="localhost:9092")
    s.add_argument("--source", choices=["pos", "app-events", "iot"], default="pos")
    s.add_argument(
        "--rate", type=float, default=5.0, help="messages per second (0 = as fast as possible)"
    )
    s.add_argument("--limit", type=int, default=None, help="stop after N messages")
    s.add_argument("--dry-run", action="store_true")
    s.set_defaults(func=cmd_stream)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
