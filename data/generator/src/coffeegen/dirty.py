"""Deliberate data-quality problems, injected at ~1.5% of rows.

Later videos (#03 dedup with window functions, #07 cleaning, #14 DLQ, #20 Great Expectations)
teach how to detect and fix exactly these:

  duplicate      — cùng transaction xuất hiện 2 lần (POS gửi lại khi mạng chập chờn)
  null_store     — store_id rỗng (máy POS mới chưa cấu hình)
  tz_drift       — timestamp ghi theo giờ local nhưng KHÔNG có offset +07:00 (bị hiểu nhầm thành UTC)
  negative_qty   — quantity âm trong line item (nhân viên "hoàn hàng" sai cách)
  late_event     — (IoT/app) bản ghi tới muộn 1–10 phút so với event time
"""

from __future__ import annotations

import random
from dataclasses import replace
from datetime import datetime

from .pos import Transaction

DIRTY_RATE = 0.015
POS_KINDS = ["duplicate", "null_store", "tz_drift", "negative_qty"]


def format_ts(ts: datetime, drift: bool = False) -> str:
    """ISO-8601 with +07:00, or — when drifting — the same wall-clock time WITHOUT offset."""
    return ts.strftime("%Y-%m-%d %H:%M:%S") if drift else ts.isoformat()


def dirty_pos(
    txns: list[Transaction], seed: int, rate: float = DIRTY_RATE
) -> list[tuple[Transaction, str | None]]:
    """Return (transaction, dirty_kind) pairs. Clean rows have kind None.

    Duplicates are appended a few rows later (not adjacent), like a real retry would land.
    """
    rng = random.Random(f"{seed}:dirty:{txns[0].ts.date().isoformat() if txns else 'empty'}")
    out: list[tuple[Transaction, str | None]] = []
    pending_dups: list[tuple[int, Transaction]] = []
    for i, t in enumerate(txns):
        kind = rng.choice(POS_KINDS) if rng.random() < rate else None
        if kind == "duplicate":
            out.append((t, None))
            pending_dups.append((i + rng.randrange(3, 40), t))
        elif kind == "null_store":
            out.append((replace(t, store_id=None), kind))
        elif kind == "negative_qty":
            lines = list(t.lines)
            j = rng.randrange(len(lines))
            bad = replace(
                lines[j], quantity=-lines[j].quantity, line_total_vnd=-lines[j].line_total_vnd
            )
            lines[j] = bad
            out.append((replace(t, lines=lines), kind))
        else:  # tz_drift or clean — tz_drift is applied at write time via the kind flag
            out.append((t, kind))
        # flush duplicates whose slot has arrived
        for slot, dup in list(pending_dups):
            if slot <= i:
                out.append((dup, "duplicate"))
                pending_dups.remove((slot, dup))
    for _, dup in pending_dups:
        out.append((dup, "duplicate"))
    return out
