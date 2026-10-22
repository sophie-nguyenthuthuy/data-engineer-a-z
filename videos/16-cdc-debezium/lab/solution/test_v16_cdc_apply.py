"""Test offline cho #16: áp dụng vài envelope Debezium giả (r/c/u/d + tombstone) vào DuckDB in-memory."""

from __future__ import annotations

import json

import duckdb
from cdc_apply import apply_envelope, init_db, unwrap


def _row(txn_id: str, amount: int, store: str = "HN01") -> dict:
    return {
        "transaction_id": txn_id,
        "store_id": store,
        "ts": "2026-02-15T03:00:00Z",
        "channel": "store",
        "payment_method": "cash",
        "customer_id": None,
        "staff_id": f"{store}-S1",
        "total_amount_vnd": amount,
        "discount_vnd": 0,
        "is_rainy_day": False,
        "loaded_at": "2026-02-15T03:00:01Z",
    }


def _env(op: str, before: dict | None, after: dict | None, ts_ms: int) -> dict:
    return {
        "before": before,
        "after": after,
        "source": {"table": "pos_transactions"},
        "op": op,
        "ts_ms": ts_ms,
    }


ENVELOPES = [
    _env("r", None, _row("HN01-20260215-00001", 45000), 1000),  # snapshot
    _env("r", None, _row("HN01-20260215-00002", 60000), 1001),
    _env("c", None, _row("HN01-20260215-99001", 45000), 2000),  # INSERT
    _env(
        "u", _row("HN01-20260215-99001", 45000), _row("HN01-20260215-99001", 50000), 2001
    ),  # UPDATE
    _env("d", _row("HN01-20260215-00002", 60000), None, 2002),  # DELETE
    _env("c", None, _row("SG01-20260215-00001", 80000, "SG01"), 2003),
]


def _apply_all(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [apply_envelope(con, e) for e in ENVELOPES]


def test_final_state_after_r_c_u_d() -> None:
    con = init_db(":memory:")
    assert _apply_all(con) == ["r", "r", "c", "u", "d", "c"]

    rows = con.execute(
        "SELECT transaction_id, total_amount_vnd, _op FROM pos_transactions ORDER BY transaction_id"
    ).fetchall()
    assert rows == [
        ("HN01-20260215-00001", 45000, "r"),
        ("HN01-20260215-99001", 50000, "u"),  # UPDATE thắng INSERT
        ("SG01-20260215-00001", 80000, "c"),
    ]  # 00002 đã bị DELETE
    assert con.execute("SELECT sum(total_amount_vnd) FROM pos_transactions").fetchone()[0] == 175000


def test_apply_is_idempotent() -> None:
    con = init_db(":memory:")
    _apply_all(con)
    before = con.execute("SELECT * FROM pos_transactions ORDER BY 1").fetchall()
    _apply_all(con)  # consumer at-least-once đọc lại toàn bộ
    assert con.execute("SELECT * FROM pos_transactions ORDER BY 1").fetchall() == before


def test_unwrap_tombstone_and_schema_wrapper() -> None:
    assert unwrap(None) is None
    flat = json.dumps(ENVELOPES[2]).encode()
    assert unwrap(flat)["op"] == "c"
    wrapped = json.dumps({"schema": {"type": "struct"}, "payload": ENVELOPES[4]}).encode()
    assert unwrap(wrapped)["op"] == "d"
