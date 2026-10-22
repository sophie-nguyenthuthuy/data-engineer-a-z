"""Ordering-app clickstream, consistent with POS app-channel orders generated from the same seed.

Funnel per session: app_open → view_menu → add_to_cart → checkout → order_placed | order_cancelled.
Sessions that end in order_placed reference a real POS transaction_id (channel = app).
Extra abandoned sessions are added so the funnel has realistic drop-off.
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta

from .calendar_effects import day_rng
from .config import DRINK_IDS, VN_TZ, Store
from .dirty import format_ts
from .pos import Transaction, simulate_day

PLATFORMS = {"android": 62, "ios": 38}
APP_VERSIONS = ["3.8.0", "3.8.1", "3.9.0", "3.9.2"]
CANCEL_REASONS = ["changed_mind", "payment_failed", "store_closed", "too_slow", "wrong_store"]
LATE_RATE = 0.01


def _uuid(rng: random.Random) -> str:
    h = f"{rng.getrandbits(128):032x}"
    return f"{h[:8]}-{h[8:12]}-4{h[13:16]}-{h[16:20]}-{h[20:32]}"


def _session_events(
    rng: random.Random, start: datetime, user: str, store_id: str, order: Transaction | None
) -> list[dict]:
    """Build one session. If `order` is given the session converts to that order."""
    sid = _uuid(rng)
    platform = rng.choices(list(PLATFORMS), weights=list(PLATFORMS.values()), k=1)[0]
    version = rng.choice(APP_VERSIONS)
    t = start

    def ev(kind: str, props: dict | None = None, late: bool = False) -> dict:
        ts = t
        return {
            "event_id": _uuid(rng),
            "ts": format_ts(ts),
            "ingested_at": format_ts(
                ts + timedelta(minutes=rng.randrange(1, 10))
                if late
                else ts + timedelta(seconds=rng.randrange(0, 3))
            ),
            "user_id": user,
            "session_id": sid,
            "event_type": kind,
            "store_id": store_id if kind != "app_open" else None,
            "platform": platform,
            "app_version": version,
            "props": props or {},
        }

    events = [ev("app_open")]
    t += timedelta(seconds=rng.randrange(2, 20))
    if order is None and rng.random() < 0.15:
        return events  # bounce
    events.append(ev("view_menu", {"screen": rng.choice(["home", "promo", "search"])}))

    if order is None:
        n_add = rng.choices([0, 1, 2], weights=[45, 40, 15], k=1)[0]
        for _ in range(n_add):
            t += timedelta(seconds=rng.randrange(5, 60))
            events.append(ev("add_to_cart", {"item_id": rng.choice(DRINK_IDS), "qty": 1}))
        if n_add and rng.random() < 0.5:
            t += timedelta(seconds=rng.randrange(5, 40))
            events.append(ev("checkout", {"cart_items": n_add}))
            if rng.random() < 0.6:
                t += timedelta(seconds=rng.randrange(5, 90))
                events.append(ev("order_cancelled", {"reason": rng.choice(CANCEL_REASONS)}))
        return events

    for line in order.lines:
        t += timedelta(seconds=rng.randrange(5, 45))
        events.append(
            ev("add_to_cart", {"item_id": line.item_id, "qty": line.quantity, "size": line.size})
        )
    t += timedelta(seconds=rng.randrange(5, 30))
    events.append(
        ev("checkout", {"cart_items": len(order.lines), "payment_method": order.payment_method})
    )
    t += timedelta(seconds=rng.randrange(3, 25))
    events.append(
        ev(
            "order_placed",
            {
                "order_id": order.transaction_id,
                "amount_vnd": order.total_amount_vnd,
                "discount_vnd": order.discount_vnd,
            },
            late=rng.random() < LATE_RATE,
        )
    )
    return events


def simulate_app_day(stores: list[Store], d: date, seed: int, scale: float = 1.0) -> list[dict]:
    rng = day_rng(seed, d, "app")
    events: list[dict] = []
    for store in stores:
        app_orders = [o for o in simulate_day(store, d, seed, scale) if o.channel == "app"]
        for order in app_orders:
            start = order.ts - timedelta(seconds=rng.randrange(120, 600))
            events.extend(
                _session_events(rng, start, order.customer_id or "anonymous", store.store_id, order)
            )
        # Abandoned sessions ≈ 70% as many as converted ones.
        for _ in range(int(len(app_orders) * 0.7)):
            hour = rng.choices(
                range(6, 23), weights=[2, 5, 5, 4, 3, 3, 3, 3, 4, 4, 4, 3, 3, 3, 3, 2, 1], k=1
            )[0]
            start = datetime(
                d.year, d.month, d.day, hour, rng.randrange(60), rng.randrange(60), tzinfo=VN_TZ
            )
            user = (
                f"C{rng.randrange(1, 2001):06d}"
                if rng.random() < 0.6
                else f"anon-{rng.getrandbits(32):08x}"
            )
            events.extend(_session_events(rng, start, user, store.store_id, None))
    # Dirty: ~1% exact duplicate events (client retry)
    dups = [e for e in events if rng.random() < 0.01]
    events.extend(dups)
    events.sort(key=lambda e: e["ingested_at"])
    return events
