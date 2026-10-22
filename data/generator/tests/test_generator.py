"""Tests for coffeegen — determinism, schema, calendar effects, dirty-data rates."""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import date
from pathlib import Path

import pytest

from coffeegen.app_events import simulate_app_day
from coffeegen.calendar_effects import tet_multiplier
from coffeegen.cli import main
from coffeegen.config import MENU, build_stores
from coffeegen.dirty import dirty_pos
from coffeegen.export import LINE_FIELDS, TXN_FIELDS
from coffeegen.iot import IOT_FIELDS, simulate_iot
from coffeegen.pos import simulate_day

STORES = build_stores(30)


def test_stores_cover_both_cities_and_are_unique():
    ids = [s.store_id for s in STORES]
    assert len(ids) == len(set(ids)) == 30
    assert sum(1 for s in STORES if s.city == "Hà Nội") == 15
    assert build_stores(4)[0].store_id == "HN01" and build_stores(4)[1].store_id == "SG01"
    assert len(build_stores(45)) == 45  # more than 30 → suffixed names, still unique
    assert len({s.store_id for s in build_stores(45)}) == 45


def test_menu_prices_are_vnd_and_positive():
    assert all(m.price_vnd > m.cost_vnd > 0 for m in MENU)
    assert all(m.price_vnd % 1000 == 0 for m in MENU)


def test_simulate_day_is_deterministic():
    a = simulate_day(STORES[0], date(2026, 3, 2), seed=42)
    b = simulate_day(STORES[0], date(2026, 3, 2), seed=42)
    assert [t.__dict__ for t in a] == [t.__dict__ for t in b]
    c = simulate_day(STORES[0], date(2026, 3, 2), seed=43)
    assert [t.transaction_id for t in a] != [t.transaction_id for t in c] or [t.ts for t in a] != [
        t.ts for t in c
    ]


def test_totals_match_line_items():
    for t in simulate_day(STORES[3], date(2026, 3, 2), seed=1):
        assert t.total_amount_vnd == sum(line.line_total_vnd for line in t.lines) - t.discount_vnd
        assert t.channel in {"store", "app"}
        if t.channel == "app":
            assert t.customer_id is not None
            assert t.payment_method != "cash"


def test_peak_hours_dominate():
    hours = [t.ts.hour for t in simulate_day(STORES[1], date(2026, 3, 3), seed=7)]
    peak = sum(1 for h in hours if 7 <= h <= 9 or 14 <= h <= 16)
    assert peak / len(hours) > 0.4
    assert not any(h < 5 or h == 23 for h in hours)


def test_tet_multiplier_shape():
    assert tet_multiplier(date(2026, 2, 17)) == 0.15  # mùng 1
    assert tet_multiplier(date(2026, 2, 16)) == 1.5  # 30 Tết
    assert 1.0 < tet_multiplier(date(2026, 2, 12)) < 1.5
    assert tet_multiplier(date(2026, 2, 20)) == 0.5  # mùng 4
    assert tet_multiplier(date(2026, 6, 1)) == 1.0


def test_tet_reduces_volume():
    normal = len(simulate_day(STORES[0], date(2026, 3, 3), seed=42))
    tet = len(simulate_day(STORES[0], date(2026, 2, 17), seed=42))
    assert tet < normal * 0.3


def test_dirty_rate_within_bounds():
    txns = simulate_day(STORES[0], date(2026, 3, 2), seed=42, scale=5)
    tagged = dirty_pos(txns, seed=42)
    kinds = [k for _, k in tagged if k]
    rate = len(kinds) / len(txns)
    assert 0.005 < rate < 0.04, rate
    assert {"duplicate", "null_store", "tz_drift", "negative_qty"} <= set(kinds)
    ids = [t.transaction_id for t, _ in tagged]
    assert len(ids) > len(set(ids))  # duplicates really present


def test_app_events_join_pos():
    d = date(2026, 3, 2)
    events = simulate_app_day(STORES[:3], d, seed=42, scale=0.3)
    order_ids = {e["props"]["order_id"] for e in events if e["event_type"] == "order_placed"}
    pos_app_ids = {
        t.transaction_id
        for s in STORES[:3]
        for t in simulate_day(s, d, 42, 0.3)
        if t.channel == "app"
    }
    assert order_ids == pos_app_ids
    assert {e["event_type"] for e in events} >= {
        "app_open",
        "view_menu",
        "add_to_cart",
        "checkout",
        "order_placed",
    }
    late = [
        e
        for e in events
        if e["ingested_at"] > e["ts"][:19] + "+07:00" and e["ingested_at"][:16] != e["ts"][:16]
    ]
    assert late, "expected some late events"


def test_iot_fields_and_monotonic_counter():
    rows = list(simulate_iot(STORES[:2], 4, date(2026, 3, 2), hours=0.5, hz=0.1, seed=42))
    assert rows and set(rows[0]) == set(IOT_FIELDS)
    by_machine: dict[str, list[int]] = {}
    seen = set()
    for r in rows:
        if r["reading_id"] in seen:
            continue  # duplicates are allowed
        seen.add(r["reading_id"])
        by_machine.setdefault(r["machine_id"], []).append(r["shots_total"])
    assert len(by_machine) == 4
    # readings are in ingestion order; sort by reading_id to check the odometer never decreases
    for m in by_machine:
        shots = [
            r["shots_total"]
            for r in sorted(
                (x for x in rows if x["machine_id"] == m), key=lambda x: x["reading_id"]
            )
        ]
        assert shots == sorted(shots)


def test_cli_pos_writes_expected_files_and_is_byte_stable(tmp_path: Path):
    args = [
        "pos",
        "--days",
        "2",
        "--stores",
        "3",
        "--scale",
        "0.2",
        "--seed",
        "42",
        "--start",
        "2026-03-02",
        "--out",
    ]
    assert main([*args, str(tmp_path / "a")]) == 0
    assert main([*args, str(tmp_path / "b")]) == 0
    for name in [
        "stores.csv",
        "menu_items.csv",
        "customers.csv",
        "pos_transactions.csv",
        "pos_line_items.csv",
    ]:
        ha = hashlib.sha256((tmp_path / "a" / name).read_bytes()).hexdigest()
        hb = hashlib.sha256((tmp_path / "b" / name).read_bytes()).hexdigest()
        assert ha == hb, name
    with (tmp_path / "a" / "pos_transactions.csv").open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames == TXN_FIELDS
        rows = list(reader)
    assert len(rows) > 50
    with (tmp_path / "a" / "pos_line_items.csv").open(encoding="utf-8") as f:
        assert csv.DictReader(f).fieldnames == LINE_FIELDS


def test_cli_app_events_and_iot(tmp_path: Path):
    assert (
        main(
            [
                "app-events",
                "--days",
                "1",
                "--stores",
                "2",
                "--scale",
                "0.2",
                "--start",
                "2026-03-02",
                "--out",
                str(tmp_path),
            ]
        )
        == 0
    )
    lines = (tmp_path / "app_events.jsonl").read_text(encoding="utf-8").splitlines()
    assert lines and all(json.loads(line)["event_id"] for line in lines)
    assert (
        main(
            [
                "iot",
                "--machines",
                "4",
                "--stores",
                "2",
                "--hz",
                "0.1",
                "--hours",
                "0.2",
                "--start",
                "2026-03-02",
                "--out",
                str(tmp_path),
            ]
        )
        == 0
    )
    assert (tmp_path / "iot_readings.csv").exists() and (tmp_path / "machines.csv").exists()


def test_cli_stream_dry_run(capsys):
    assert (
        main(
            [
                "stream",
                "--dry-run",
                "--limit",
                "3",
                "--rate",
                "0",
                "--stores",
                "2",
                "--scale",
                "0.2",
                "--start",
                "2026-03-02",
            ]
        )
        == 0
    )
    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 3
    key, payload = out[0].split("\t", 1)
    assert key.startswith(("HN", "SG")) or key == ""
    assert "items" in json.loads(payload)


@pytest.mark.parametrize("n", [1, 30, 31])
def test_build_stores_sizes(n):
    assert len(build_stores(n)) == n
