"""Test offline cho #14: file .avsc là JSON hợp lệ và khớp SCHEMA.md (không có fastavro trong .venv)."""

from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).parent
TXN_FIELDS = {
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
    "items",
}
ITEM_FIELDS = {"item_id", "size", "qty", "unit_price_vnd"}


def _load(name: str) -> dict:
    return json.loads((HERE / name).read_text(encoding="utf-8"))


def _fields(schema: dict) -> dict[str, dict]:
    return {f["name"]: f for f in schema["fields"]}


def test_solution_schema_matches_schema_md() -> None:
    s = _load("pos_transaction.avsc")
    assert s["type"] == "record" and s["name"] == "PosTransaction"
    fields = _fields(s)
    assert set(fields) == TXN_FIELDS

    # nullable = union với "null" + default null
    for name in ("store_id", "customer_id"):
        assert fields[name]["type"][0] == "null" and fields[name]["default"] is None
    assert fields["total_amount_vnd"]["type"] == "long"
    assert fields["is_rainy_day"]["type"] == "boolean"

    items = fields["items"]["type"]
    assert items["type"] == "array" and items["items"]["type"] == "record"
    assert {f["name"] for f in items["items"]["fields"]} == ITEM_FIELDS


def test_v2_compatible_adds_field_with_default() -> None:
    base, v2 = (
        _fields(_load("pos_transaction.avsc")),
        _fields(_load("pos_transaction_v2_compatible.avsc")),
    )
    added = set(v2) - set(base)
    assert added == {"pos_version"}
    assert "default" in v2["pos_version"]  # không có default → reader cũ fail → không BACKWARD


def test_v2_incompatible_changes_type() -> None:
    base, bad = (
        _fields(_load("pos_transaction.avsc")),
        _fields(_load("pos_transaction_v2_incompatible.avsc")),
    )
    assert set(base) == set(bad)
    assert (
        base["total_amount_vnd"]["type"] == "long" and bad["total_amount_vnd"]["type"] == "string"
    )


def test_starter_is_valid_json_subset() -> None:
    starter = _fields(_load("../pos_transaction.avsc"))
    assert set(starter) <= TXN_FIELDS
    assert "TODO" in _load("../pos_transaction.avsc")["doc"]
