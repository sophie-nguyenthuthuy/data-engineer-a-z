"""POS transactions + line items for every store and day."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from .calendar_effects import day_rng, is_rainy, tet_multiplier, weekend_multiplier
from .config import (
    APP_SHARE,
    BASE_TXN_PER_STORE_PER_DAY,
    HOURLY_PROFILE,
    MENU,
    PAYMENT_WEIGHTS_APP,
    PAYMENT_WEIGHTS_STORE,
    SIZE_WEIGHTS,
    SIZES,
    VN_TZ,
    Store,
)

N_CUSTOMERS = 2_000
N_STAFF_PER_STORE = 6


@dataclass
class LineItem:
    line_id: str
    transaction_id: str
    item_id: str
    size: str
    quantity: int
    unit_price_vnd: int
    line_total_vnd: int


@dataclass
class Transaction:
    transaction_id: str
    store_id: str | None
    ts: datetime
    channel: str  # store | app
    payment_method: str
    customer_id: str | None
    staff_id: str
    total_amount_vnd: int
    discount_vnd: int
    is_rainy_day: bool
    lines: list[LineItem] = field(default_factory=list)


def _weighted(rng: random.Random, weights: dict[str, int]) -> str:
    return rng.choices(list(weights), weights=list(weights.values()), k=1)[0]


def _pick_hour(rng: random.Random) -> int:
    return rng.choices(range(24), weights=HOURLY_PROFILE, k=1)[0]


def _random_ts(rng: random.Random, d: date, rainy: bool) -> datetime:
    hour = _pick_hour(rng)
    # Rainy afternoons shift traffic later and thin the 14–16 peak a bit.
    if rainy and 14 <= hour <= 16 and rng.random() < 0.3:
        hour += 2
    minute, second = rng.randrange(60), rng.randrange(60)
    return datetime(d.year, d.month, d.day, hour, minute, second, tzinfo=VN_TZ)


def _build_lines(rng: random.Random, txn_id: str, channel: str) -> list[LineItem]:
    n_items = rng.choices([1, 2, 3, 4], weights=[55, 30, 11, 4], k=1)[0]
    if channel == "app":
        n_items = min(n_items + rng.choice([0, 0, 1]), 5)  # app orders are slightly bigger
    weights = [m.popularity for m in MENU]
    chosen = rng.choices(MENU, weights=weights, k=n_items)
    lines: list[LineItem] = []
    for i, item in enumerate(chosen, start=1):
        size = "S" if item.category == "food" else _weighted(rng, SIZE_WEIGHTS)
        qty = rng.choices([1, 2, 3], weights=[80, 16, 4], k=1)[0]
        unit = item.price_vnd + SIZES[size]
        lines.append(LineItem(f"{txn_id}-{i}", txn_id, item.item_id, size, qty, unit, unit * qty))
    return lines


def simulate_day(store: Store, d: date, seed: int, scale: float = 1.0) -> list[Transaction]:
    """All clean transactions of one store on one day. Deterministic for (store, d, seed, scale)."""
    rng = day_rng(seed, d, store.store_id)
    rainy = is_rainy(d, store.city, rng)

    expected = BASE_TXN_PER_STORE_PER_DAY * store.tier * scale
    expected *= tet_multiplier(d) * weekend_multiplier(d, store.city)
    if rainy:
        expected *= 0.85  # fewer walk-ins overall; app share goes up below
    n = max(0, int(rng.gauss(expected, expected * 0.08)))

    app_share = min(0.7, APP_SHARE * (1.3 if rainy else 1.0))
    txns: list[Transaction] = []
    for seq in range(1, n + 1):
        txn_id = f"{store.store_id}-{d:%Y%m%d}-{seq:05d}"
        channel = "app" if rng.random() < app_share else "store"
        payment = _weighted(rng, PAYMENT_WEIGHTS_APP if channel == "app" else PAYMENT_WEIGHTS_STORE)
        if channel == "app" or rng.random() < 0.35:
            customer: str | None = f"C{rng.randrange(1, N_CUSTOMERS + 1):06d}"
        else:
            customer = None
        staff = f"{store.store_id}-S{rng.randrange(1, N_STAFF_PER_STORE + 1)}"
        lines = _build_lines(rng, txn_id, channel)
        subtotal = sum(line.line_total_vnd for line in lines)
        discount = 0
        if channel == "app" and rng.random() < 0.25:
            discount = min(subtotal, rng.choice([10_000, 15_000, 20_000]))
        txns.append(
            Transaction(
                transaction_id=txn_id,
                store_id=store.store_id,
                ts=_random_ts(rng, d, rainy),
                channel=channel,
                payment_method=payment,
                customer_id=customer,
                staff_id=staff,
                total_amount_vnd=subtotal - discount,
                discount_vnd=discount,
                is_rainy_day=rainy,
                lines=lines,
            )
        )
    txns.sort(key=lambda t: t.ts)
    return txns


def simulate_range(stores: list[Store], start: date, days: int, seed: int, scale: float = 1.0):
    """Yield (day, transactions) for each day in order — streaming-friendly, never holds 90 days in RAM."""
    for offset in range(days):
        d = start + timedelta(days=offset)
        day_txns: list[Transaction] = []
        for store in stores:
            day_txns.extend(simulate_day(store, d, seed, scale))
        day_txns.sort(key=lambda t: t.ts)
        yield d, day_txns


def customers(seed: int) -> list[dict]:
    """Customer dimension — 2,000 loyalty members with a few PII fields (video #23 teaches masking them)."""
    rng = random.Random(f"{seed}:customers")
    first = [
        "An",
        "Bình",
        "Chi",
        "Dũng",
        "Hà",
        "Hùng",
        "Lan",
        "Linh",
        "Minh",
        "Nam",
        "Ngọc",
        "Phương",
        "Quân",
        "Thảo",
        "Trang",
        "Tuấn",
    ]
    last = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Vũ", "Đặng", "Bùi", "Đỗ", "Hồ"]
    out = []
    for i in range(1, N_CUSTOMERS + 1):
        name = f"{rng.choice(last)} {rng.choice(['Văn', 'Thị', 'Minh', 'Thu', 'Hữu', 'Ngọc'])} {rng.choice(first)}"
        phone = f"09{rng.randrange(10_000_000, 99_999_999)}"
        joined = date(2022, 1, 1) + timedelta(days=rng.randrange(0, 1400))
        out.append(
            {
                "customer_id": f"C{i:06d}",
                "full_name": name,
                "phone": phone,
                "email": f"user{i}@example.com",
                "city": rng.choice(["Hà Nội", "TP.HCM"]),
                "joined_date": joined.isoformat(),
                "tier": rng.choices(["bronze", "silver", "gold"], weights=[70, 25, 5], k=1)[0],
            }
        )
    return out
