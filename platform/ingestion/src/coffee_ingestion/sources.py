"""Read raw data from the source systems.

Two sources today: a folder of CSV/JSONL (data/samples or generator output) and Postgres (the POS OLTP db).
Both return Polars DataFrames with **every column as string** — bronze keeps data exactly as received;
typing happens in silver.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

POS_TABLES = ("stores", "menu_items", "customers", "pos_transactions", "pos_line_items")


def read_csv_raw(path: Path) -> pl.DataFrame:
    """All columns as Utf8, empty string → null. Never let the CSV reader guess types for bronze."""
    return pl.read_csv(path, infer_schema=False, null_values=[""], encoding="utf8")


def read_folder(folder: Path, table: str) -> pl.DataFrame:
    f = folder / f"{table}.csv"
    if not f.exists():
        raise FileNotFoundError(f"missing source file {f}")
    return read_csv_raw(f)


def read_postgres(url: str, table: str) -> pl.DataFrame:
    """Pull a whole table from Postgres as strings (fine for a 30-store chain; video #16 shows CDC for scale)."""
    import psycopg  # optional dependency: coffee-ingestion[postgres]

    with psycopg.connect(url) as conn, conn.cursor() as cur:
        # `ts` là TIMESTAMPTZ: psycopg trả về theo TimeZone của session (mặc định UTC trong container).
        # Ép về giờ VN để chuỗi str(ts) có dạng '2026-02-15 05:57:28+07:00' giống CSV — pipeline cắt 10 ký tự
        # đầu làm ngày nghiệp vụ, nên nếu để UTC thì hoá đơn 5h sáng 15/02 sẽ rơi vào partition 14/02.
        cur.execute("SET TIME ZONE 'Asia/Ho_Chi_Minh'")
        cur.execute(f"SELECT * FROM {table}")  # noqa: S608 — table names come from POS_TABLES, not user input
        cols = [d.name for d in cur.description]
        rows = cur.fetchall()
    data = {
        c: [None if v is None else str(v) for v in (r[i] for r in rows)] for i, c in enumerate(cols)
    }
    return pl.DataFrame(data, schema={c: pl.Utf8 for c in cols})
