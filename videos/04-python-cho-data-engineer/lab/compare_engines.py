"""Lab #04 — cùng một pipeline nhỏ viết 3 lần: pandas, Polars, DuckDB. So kết quả và thời gian.

Pipeline: đọc pos_transactions.csv → parse ts (có hoặc không có +07:00) → bỏ store_id NULL
          → lọc business_date (giờ VN) = 2026-02-15 → doanh thu theo cửa hàng.
Chưa dedup (đó là việc của silver, video #07) — ở đây chỉ so 3 engine trên cùng một logic.

Chạy:
    uv run python videos/04-python-cho-data-engineer/lab/compare_engines.py
    uv run python videos/04-python-cho-data-engineer/lab/compare_engines.py --date 2026-02-16

Mỗi hàm trả về list[(store_id, revenue_vnd)] sắp theo store_id, để 3 hàm so được với nhau bằng ==.
Điền TODO theo thứ tự: pandas → polars → duckdb. Hàm nào chưa làm thì raise NotImplementedError, script bỏ qua.
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

# Dùng lại code của platform: silver.py đã giải bài tz_drift; sources.py có reader "mọi cột là chuỗi".
from coffee_ingestion.silver import parse_vn_timestamp
from coffee_ingestion.sources import read_csv_raw

REPO = Path(__file__).resolve().parents[3]
SAMPLES = REPO / "data" / "samples" / "pos_transactions.csv"
VN_TZ = "Asia/Ho_Chi_Minh"
OFFSET_RE = r"[+-]\d{2}:\d{2}$"  # chuỗi kết thúc bằng +07:00 / -05:00 …

Result = list[tuple[str, int]]


def revenue_pandas(path: Path, business_date: date) -> Result:
    """pandas — eager: mỗi dòng chạy ngay, tạo DataFrame mới trong RAM."""
    df = pd.read_csv(path, dtype=str)  # dtype=str: không đoán kiểu; ô trống → NaN
    # TODO 1: parse ts.
    #   has_offset = df["ts"].str.contains(OFFSET_RE)
    #   fixed = df["ts"].where(has_offset, df["ts"].str.replace(" ", "T") + "+07:00")
    #   df["ts_utc"] = pd.to_datetime(fixed, utc=True, format="ISO8601")
    # TODO 2: bỏ store_id NaN (df["store_id"].notna()), lọc ngày:
    #   df["ts_utc"].dt.tz_convert(VN_TZ).dt.date == business_date
    # TODO 3: groupby("store_id"), sum total_amount_vnd (nhớ astype("int64") trước khi cộng — nó đang là chuỗi!)
    #   return sorted((str(k), int(v)) for k, v in out.items())
    raise NotImplementedError(f"đã đọc {len(df)} dòng — làm TODO 1–3")


def revenue_polars(path: Path, business_date: date) -> Result:
    """Polars — lazy: không gì chạy cho tới .collect(); Polars tối ưu cả chuỗi phép biến đổi trước."""
    lf = read_csv_raw(path).lazy()
    # TODO 4: .with_columns(parse_vn_timestamp("ts").alias("ts_utc"))  ← hàm của silver.py
    # TODO 5: .filter(pl.col("store_id").is_not_null())
    #         .with_columns(pl.col("ts_utc").dt.convert_time_zone(VN_TZ).dt.date().alias("business_date"))
    #         .filter(pl.col("business_date") == business_date)
    # TODO 6: .group_by("store_id").agg(pl.col("total_amount_vnd").cast(pl.Int64).sum().alias("revenue_vnd")).sort("store_id")
    #         df = lf.collect(); return [(r[0], int(r[1])) for r in df.iter_rows()]
    #   Thử in lf.explain() trước khi collect để thấy plan đã được tối ưu (filter đẩy xuống đâu?).
    raise NotImplementedError(f"LazyFrame có {len(lf.collect_schema())} cột — làm TODO 4–6")


def revenue_duckdb(path: Path, business_date: date) -> Result:
    """DuckDB — SQL trực tiếp trên file CSV, không qua DataFrame nào."""
    # TODO 7: viết SQL. Gợi ý:
    #   FROM read_csv('{path}', all_varchar=true, nullstr='')
    #   ts_utc = (CASE WHEN regexp_matches(ts, '{OFFSET_RE}') THEN ts ELSE replace(ts, ' ', 'T') || '+07:00' END)::TIMESTAMPTZ
    #   WHERE store_id IS NOT NULL AND (ts_utc AT TIME ZONE '{VN_TZ}')::DATE = DATE '{business_date.isoformat()}'
    #   GROUP BY store_id ORDER BY store_id
    #   return [(r[0], int(r[1])) for r in duckdb.sql(sql).fetchall()]
    raise NotImplementedError


ENGINES: dict[str, Callable[[Path, date], Result]] = {
    "pandas": revenue_pandas,
    "polars": revenue_polars,
    "duckdb": revenue_duckdb,
}


def bench(
    fn: Callable[[Path, date], Result], path: Path, d: date, repeat: int = 5
) -> tuple[Result, float]:
    """Chạy `repeat` lần, trả về (kết quả, trung vị ms)."""
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
        try:
            out, ms = bench(fn, path, d)
        except NotImplementedError:
            print(f"{name:<7} chưa làm (TODO)")
            continue
        results[name] = out
        total = sum(v for _, v in out)
        print(f"{name:<7} {len(out):>3} cửa hàng  tổng {total:>12,} VND  {ms:7.1f} ms")

    done = list(results.values())
    if len(done) >= 2:
        assert all(r == done[0] for r in done), "các engine phải ra cùng kết quả"
        print(f"✓ {len(done)} engine cho cùng kết quả")


if __name__ == "__main__":
    main()
