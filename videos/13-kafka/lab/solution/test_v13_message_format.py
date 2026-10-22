"""Test offline cho #13: message mà `coffeegen stream` sẽ gửi — không cần Kafka.

Dùng đúng generator của producer (coffeegen.stream._messages) cho ngày chuẩn 2026-02-15, scale 0.1.
"""

from __future__ import annotations

from datetime import date

from coffeegen.stream import _messages

REQUIRED = {
    "transaction_id",
    "store_id",
    "ts",
    "channel",
    "payment_method",
    "total_amount_vnd",
    "items",
}


def test_pos_messages_keyed_by_store_with_items() -> None:
    msgs = list(_messages("pos", date(2026, 2, 15), stores_n=30, seed=42, scale=0.1))
    assert len(msgs) == 1008

    for key, payload in msgs:
        assert key == (payload["store_id"] or "")  # key = store_id, null → ""
        assert payload.keys() >= REQUIRED
        assert isinstance(payload["items"], list) and payload["items"]
        assert payload["items"][0].keys() >= {"item_id", "size", "qty", "unit_price_vnd"}

    assert sum(1 for key, _ in msgs if key == "") == 7  # null_store cố ý
    assert len({m["transaction_id"] for _, m in msgs}) == 1004  # 4 duplicate cố ý
    assert sum(m["total_amount_vnd"] for _, m in msgs) == 97_494_000
