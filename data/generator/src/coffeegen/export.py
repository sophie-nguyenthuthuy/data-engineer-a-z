"""Turn simulation objects into flat rows for CSV/JSONL/Kafka."""

from __future__ import annotations

from .dirty import format_ts
from .pos import Transaction

TXN_FIELDS = [
    "transaction_id",
    "store_id",
    "ts",
    "channel",
    "payment_method",
    "customer_id",
    "staff_id",
    "total_amount_vnd",
    "discount_vnd",
    "is_rainy_day",
]
LINE_FIELDS = [
    "line_id",
    "transaction_id",
    "item_id",
    "size",
    "quantity",
    "unit_price_vnd",
    "line_total_vnd",
]
STORE_FIELDS = ["store_id", "store_name", "city", "district", "opened_date", "seats", "tier"]
MENU_FIELDS = ["item_id", "item_name", "category", "price_vnd", "cost_vnd"]
CUSTOMER_FIELDS = ["customer_id", "full_name", "phone", "email", "city", "joined_date", "tier"]


def txn_row(t: Transaction, dirty_kind: str | None) -> dict:
    return {
        "transaction_id": t.transaction_id,
        "store_id": t.store_id,
        "ts": format_ts(t.ts, drift=(dirty_kind == "tz_drift")),
        "channel": t.channel,
        "payment_method": t.payment_method,
        "customer_id": t.customer_id,
        "staff_id": t.staff_id,
        "total_amount_vnd": t.total_amount_vnd,
        "discount_vnd": t.discount_vnd,
        "is_rainy_day": str(t.is_rainy_day).lower(),
    }


def line_rows(t: Transaction) -> list[dict]:
    return [line.__dict__ for line in t.lines]


def txn_event(t: Transaction, dirty_kind: str | None) -> dict:
    """Nested JSON message for Kafka topic pos.transactions."""
    row = txn_row(t, dirty_kind)
    row["items"] = [
        {
            "item_id": line.item_id,
            "size": line.size,
            "qty": line.quantity,
            "unit_price_vnd": line.unit_price_vnd,
        }
        for line in t.lines
    ]
    return row
