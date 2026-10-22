"""Static reference data: stores, menu, payment mix, calendar constants.

Everything here is deliberately boring and readable — learners will open this file
to understand what the dataset "means".
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta, timezone

VN_TZ = timezone(timedelta(hours=7), name="Asia/Ho_Chi_Minh")
BRAND = "Phin Phố"

# ── Stores ────────────────────────────────────────────────────────────────
# 15 quận ở Hà Nội + 15 quận ở TP.HCM. Thứ tự này quyết định store_id (HN01…, SG01…).
HANOI_DISTRICTS = [
    "Hoàn Kiếm",
    "Ba Đình",
    "Đống Đa",
    "Hai Bà Trưng",
    "Cầu Giấy",
    "Tây Hồ",
    "Thanh Xuân",
    "Hoàng Mai",
    "Long Biên",
    "Nam Từ Liêm",
    "Bắc Từ Liêm",
    "Hà Đông",
    "Thanh Trì",
    "Gia Lâm",
    "Đông Anh",
]
HCMC_DISTRICTS = [
    "Quận 1",
    "Quận 3",
    "Quận 4",
    "Quận 5",
    "Quận 7",
    "Quận 10",
    "Quận 11",
    "Bình Thạnh",
    "Phú Nhuận",
    "Tân Bình",
    "Gò Vấp",
    "Thủ Đức",
    "Bình Tân",
    "Tân Phú",
    "Quận 12",
]


@dataclass(frozen=True)
class Store:
    store_id: str
    store_name: str
    city: str  # "Hà Nội" | "TP.HCM"
    district: str
    opened_date: date
    seats: int
    tier: float  # traffic multiplier, 0.6–1.5


def build_stores(n: int) -> list[Store]:
    """Return the first `n` stores, alternating Hà Nội / TP.HCM so small `n` covers both cities."""
    districts: list[tuple[str, str]] = []
    for hn, sg in zip(HANOI_DISTRICTS, HCMC_DISTRICTS, strict=True):
        districts.append(("Hà Nội", hn))
        districts.append(("TP.HCM", sg))

    stores: list[Store] = []
    hn_i = sg_i = 0
    for i in range(n):
        city, district = districts[i % len(districts)]
        suffix = "" if i < len(districts) else f" {i // len(districts) + 1}"
        if city == "Hà Nội":
            hn_i += 1
            sid = f"HN{hn_i:02d}"
        else:
            sg_i += 1
            sid = f"SG{sg_i:02d}"
        # Deterministic "character" per store derived from its index, no RNG needed.
        tier = round(0.6 + ((i * 7) % 10) / 10.0, 2)  # 0.6 … 1.5
        seats = 24 + (i * 13) % 60
        opened = date(2019, 1, 15) + timedelta(days=(i * 97) % 2000)
        stores.append(
            Store(sid, f"{BRAND} {district}{suffix}", city, district, opened, seats, tier)
        )
    return stores


# ── Menu ──────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class MenuItem:
    item_id: str
    item_name: str
    category: str  # coffee | tea | milk_tea | other_drink | food
    price_vnd: int
    cost_vnd: int
    popularity: int  # relative weight when picking items


MENU: list[MenuItem] = [
    MenuItem("CF01", "Cà phê đen đá", "coffee", 29_000, 7_000, 14),
    MenuItem("CF02", "Cà phê sữa đá", "coffee", 35_000, 9_000, 20),
    MenuItem("CF03", "Bạc xỉu", "coffee", 39_000, 11_000, 16),
    MenuItem("CF04", "Cà phê muối", "coffee", 45_000, 12_000, 9),
    MenuItem("CF05", "Cà phê trứng", "coffee", 55_000, 15_000, 4),
    MenuItem("CF06", "Cold brew", "coffee", 49_000, 13_000, 6),
    MenuItem("CF07", "Espresso", "coffee", 39_000, 9_000, 5),
    MenuItem("CF08", "Americano", "coffee", 45_000, 9_000, 7),
    MenuItem("CF09", "Cappuccino", "coffee", 55_000, 14_000, 6),
    MenuItem("CF10", "Latte", "coffee", 59_000, 15_000, 6),
    MenuItem("TE01", "Trà đào cam sả", "tea", 49_000, 13_000, 12),
    MenuItem("TE02", "Trà vải", "tea", 49_000, 13_000, 7),
    MenuItem("TE03", "Trà sen vàng", "tea", 45_000, 11_000, 6),
    MenuItem("TE04", "Trà ô long", "tea", 39_000, 9_000, 4),
    MenuItem("MT01", "Trà sữa trân châu", "milk_tea", 45_000, 12_000, 9),
    MenuItem("MT02", "Matcha latte", "milk_tea", 59_000, 17_000, 5),
    MenuItem("OD01", "Sữa chua đá", "other_drink", 39_000, 10_000, 4),
    MenuItem("OD02", "Nước cam", "other_drink", 45_000, 15_000, 4),
    MenuItem("FD01", "Bánh mì que", "food", 15_000, 6_000, 8),
    MenuItem("FD02", "Croissant", "food", 35_000, 12_000, 5),
    MenuItem("FD03", "Bánh su kem", "food", 25_000, 8_000, 4),
    MenuItem("FD04", "Bánh chuối", "food", 29_000, 9_000, 3),
    MenuItem("FD05", "Tiramisu", "food", 49_000, 18_000, 3),
]
MENU_BY_ID = {m.item_id: m for m in MENU}
DRINK_IDS = [m.item_id for m in MENU if m.category != "food"]

SIZES = {"S": 0, "M": 5_000, "L": 10_000}  # price surcharge in VND
SIZE_WEIGHTS = {"S": 25, "M": 55, "L": 20}

# ── Payments & channels ───────────────────────────────────────────────────
PAYMENT_WEIGHTS_STORE = {
    "cash": 35,
    "momo": 25,
    "card": 15,
    "vnpay": 10,
    "zalopay": 10,
    "app_wallet": 5,
}
PAYMENT_WEIGHTS_APP = {"momo": 40, "app_wallet": 25, "card": 15, "vnpay": 10, "zalopay": 10}
APP_SHARE = 0.35  # fraction of orders that come from the ordering app

# ── Calendar effects ──────────────────────────────────────────────────────
# Tết Nguyên Đán (mùng 1) for the years the course may generate.
TET_MUNG_1 = {
    2024: date(2024, 2, 10),
    2025: date(2025, 1, 29),
    2026: date(2026, 2, 17),
    2027: date(2027, 2, 6),
    2028: date(2028, 1, 26),
}
# Hourly traffic profile 0..23 — peaks 7–9 and 14–16, closed 23–5.
HOURLY_PROFILE = [
    0,
    0,
    0,
    0,
    0,
    0.3,  # 0–5
    1.8,
    4.5,
    4.8,
    3.6,  # 6–9  (morning peak 7–9)
    2.2,
    2.0,
    1.8,  # 10–12
    1.6,
    3.4,
    3.8,
    3.0,  # 13–16 (afternoon peak 14–16)
    2.2,
    2.0,
    2.4,
    2.4,
    1.6,
    0.6,
    0,  # 17–23
]
BASE_TXN_PER_STORE_PER_DAY = 200

# Rainy-season probability of a rainy day, by month (1..12).
RAIN_PROB_HANOI = [0.10, 0.12, 0.15, 0.20, 0.35, 0.45, 0.55, 0.55, 0.40, 0.25, 0.12, 0.08]
RAIN_PROB_HCMC = [0.03, 0.02, 0.05, 0.15, 0.45, 0.60, 0.60, 0.60, 0.65, 0.60, 0.35, 0.10]
