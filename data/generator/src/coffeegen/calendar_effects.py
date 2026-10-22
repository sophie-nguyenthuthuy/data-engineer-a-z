"""Calendar effects: Tết, weekends, rainy days. Pure functions, easy to unit-test."""

from __future__ import annotations

import random
from datetime import date

from .config import RAIN_PROB_HANOI, RAIN_PROB_HCMC, TET_MUNG_1


def tet_multiplier(d: date) -> float:
    """Traffic multiplier around Tết.

    - 7 ngày trước Tết: mua sắm, tụ tập → tăng dần tới ×1.5 ngày 30 Tết
    - Mùng 1–3: hầu hết cửa hàng đóng → ×0.15
    - Mùng 4–6: mở lại dần → ×0.5
    """
    mung1 = TET_MUNG_1.get(d.year)
    if mung1 is None:
        return 1.0
    delta = (d - mung1).days
    if -7 <= delta <= -1:
        return 1.0 + 0.5 * (7 + delta) / 6  # 1.0 (7 ngày trước) → 1.5 (30 Tết)
    if 0 <= delta <= 2:
        return 0.15
    if 3 <= delta <= 5:
        return 0.5
    return 1.0


def weekend_multiplier(d: date, city: str) -> float:
    if d.weekday() >= 5:  # Sat/Sun
        return 1.2 if city == "TP.HCM" else 1.1
    return 1.0


def is_rainy(d: date, city: str, rng: random.Random) -> bool:
    probs = RAIN_PROB_HANOI if city == "Hà Nội" else RAIN_PROB_HCMC
    return rng.random() < probs[d.month - 1]


def day_rng(seed: int, d: date, salt: str) -> random.Random:
    """A fresh RNG per (seed, day, salt) so every day is independent and reproducible."""
    return random.Random(f"{seed}:{d.isoformat()}:{salt}")
