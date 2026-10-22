"""Lời giải #04 — cùng một pipeline nhỏ viết 3 lần: pandas, Polars, DuckDB.

Pipeline: đọc pos_transactions.csv → parse ts (có hoặc không có +07:00) → bỏ store_id NULL
          → lọc business_date (giờ VN) = 2026-02-15 → doanh thu theo cửa hàng.
Chưa dedup (đó là việc của silver, video #07) — ở đây chỉ so 3 engine trên cùng một logic.

Chạy:
    uv run python videos/04-python-cho-data-engineer/lab/solution/compare_engines.py
    uv run python videos/04-python-cho-data-engineer/lab/solution/compare_engines.py --date 2026-02-16
"""

from __future__ import annotations

import argparse
import statistics
import time
from collections.abc import Callable
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl

from coffee_ingestion.silver import parse_vn_timestamp
from coffee_ingestion.sources import read_csv_raw

REPO = Path(__file__).resolve().parents[4]
SAMPLES = REPO / "data" / "samples" / "pos_transactions.csv"
VN_TZ = "Asia/Ho_Chi_Minh"
OFFSET_RE = r"[+-]\d{2}:\d{2}$"

Result = list[tuple[str, int]]  # [(store_id, revenue_vnd)] sắp theo store_id


def revenue_pandas(path: Path, business_date: date) -> Result:
    """pandas: eager, mỗi dòng dưới đây chạy ngay và tạo một DataFrame/Series mới trong RAM."""
    df = pd.read_csv(path, dtype=str)  # dtype=str: không đoán kiểu (giống bronze); ô trống → NaN
    has_offset = df["ts"].str.contains(OFFSET_RE)
    fixed = df["ts"].where(has_offset, df["ts"].str.replace(" ", "T") + "+07:00")
    df["ts_utc"] = pd.to_datetime(fixed, utc=True, format="ISO8601")
    df = df[df["store_id"].notna()]
    df = df[df["ts_utc"].dt.tz_convert(VN_TZ).dt.date == business_date]
    out = df.groupby("store_id")["total_amount_vnd"].apply(lambda s: s.astype("int64").sum())
    return sorted((str(k), int(v)) for k, v in out.items())


def revenue_polars(path: Path, business_date: date) -> Result:
    """Polars: lazy. Không gì chạy cho tới .collect() — Polars nhìn cả chuỗi phép biến đổi rồi tối ưu
    (đẩy filter xuống sát chỗ đọc, chỉ đọc cột cần). parse_vn_timestamp dùng lại từ platform/ingestion."""
    lf = (
        read_csv_raw(path)  # cùng reader với coffee-ingest: mọi cột Utf8, "" → null
        .lazy()
        .with_columns(parse_vn_timestamp("ts").alias("ts_utc"))
        .filter(pl.col("store_id").is_not_null())
        .with_columns(pl.col("ts_utc").dt.convert_time_zone(VN_TZ).dt.date().alias("business_date"))
        .filter(pl.col("business_date") == business_date)
        .group_by("store_id")
        .agg(pl.col("total_amount_vnd").cast(pl.Int64).sum().alias("revenue_vnd"))
        .sort("store_id")
    )
    df = lf.collect()
    return [(r[0], int(r[1])) for r in df.iter_rows()]


def revenue_duckdb(path: Path, business_date: date) -> Result:
    """DuckDB: SQL, cũng lazy (planner + vectorized execution), đọc thẳng CSV không qua DataFrame."""
    sql = f"""
        WITH typed AS (
            SELECT
                store_id,
                (CASE WHEN regexp_matches(ts, '{OFFSET_RE}') THEN ts
                      ELSE replace(ts, ' ', 'T') || '+07:00' END)::TIMESTAMPTZ AS ts_utc,
                total_amount_vnd::BIGINT AS amount
            FROM read_csv('{path}', all_varchar=true, nullstr='')
            WHERE store_id IS NOT NULL
        )
        SELECT store_id, sum(amount) AS revenue_vnd
        FROM typed
        WHERE (ts_utc AT TIME ZONE '{VN_TZ}')::DATE = DATE '{business_date.isoformat()}'
        GROUP BY 1
        ORDER BY 1
    """
    return [(r[0], int(r[1])) for r in duckdb.sql(sql).fetchall()]


ENGINES: dict[str, Callable[[Path, date], Result]] = {
    "pandas": revenue_pandas,
    "polars": revenue_polars,
    "duckdb": revenue_duckdb,
}


def bench(
    fn: Callable[[Path, date], Result], path: Path, d: date, repeat: int = 5
) -> tuple[Result, float]:
    durations: list[float] = []
    out: Result = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        out = fn(path, d)
        durations.append((time.perf_counter() - t0) * 1000)
    return out, statistics.median(durations)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=str(SAMPLES))
    ap.add_argument("--date", default="2026-02-15")
    args = ap.parse_args()
    path, d = Path(args.source), date.fromisoformat(args.date)

    results: dict[str, Result] = {}
    for name, fn in ENGINES.items():
        out, ms = bench(fn, path, d)
        results[name] = out
        total = sum(v for _, v in out)
        print(f"{name:<7} {len(out):>3} cửa hàng  tổng {total:>12,} VND  {ms:7.1f} ms")

    assert results["pandas"] == results["polars"] == results["duckdb"], (
        "3 engine phải ra cùng kết quả"
    )
    print("✓ 3 engine cho cùng kết quả")

    # Vì sao phải parse offset cẩn thận: nếu coi ts thiếu offset là UTC, hoá đơn 21:06 ngày 14 trôi sang 04:06 ngày 15.
    wrong = duckdb.sql(
        f"""SELECT sum(total_amount_vnd::BIGINT) FROM read_csv('{path}', all_varchar=true, nullstr='')
            WHERE store_id IS NOT NULL
              AND ((replace(ts, ' ', 'T') || CASE WHEN regexp_matches(ts, '{OFFSET_RE}') THEN '' ELSE '+00:00' END)::TIMESTAMPTZ
                   AT TIME ZONE '{VN_TZ}')::DATE = DATE '{d.isoformat()}'"""
    ).fetchall()[0][0]
    right = sum(v for _, v in results["duckdb"])
    print(f"Nếu đọc sai tz: {wrong:,} VND thay vì {right:,} VND (lệch {wrong - right:+,})")


if __name__ == "__main__":
    main()
