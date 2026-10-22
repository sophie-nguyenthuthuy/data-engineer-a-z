"""Lab #02 — cùng một câu hỏi, hai cách lưu trữ.

Câu hỏi nghiệp vụ: doanh thu theo ngày × cửa hàng.
  - OLTP / row store: Postgres (chạy riêng bằng lab/explain.sql trong Docker)
  - OLAP / column store: DuckDB đọc CSV (row-oriented text) rồi đọc Parquet (columnar)

Chạy:
    uv run python videos/02-kien-truc-du-lieu/lab/oltp_vs_olap.py
    uv run python videos/02-kien-truc-du-lieu/lab/oltp_vs_olap.py --source ./out   # dữ liệu 90 ngày (xem README)

Điền các TODO theo thứ tự. Mỗi TODO 1–3 dòng.
"""

from __future__ import annotations

import argparse
import statistics
import time
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[3]

# Câu hỏi nghiệp vụ. Dùng substr(ts, 1, 10) làm business_date vì ts trong file CSV là giờ local
# (có hoặc không có +07:00 — video #03 xử lý kỹ hơn). Bỏ hoá đơn không có store_id.
REVENUE_SQL = """
-- TODO 1: viết câu SQL doanh thu theo ngày × cửa hàng trên bảng {src}
--   cột: business_date (substr(ts, 1, 10)), store_id, revenue_vnd (sum total_amount_vnd::BIGINT)
--   where store_id is not null, group by 1, 2, order by 1, 2
SELECT 1 FROM {src} LIMIT 0
"""


def csv_to_parquet(csv_path: Path, parquet_path: Path) -> None:
    """Chuyển CSV → Parquet bằng DuckDB (giữ mọi cột là VARCHAR để không đoán kiểu — như bronze)."""
    # TODO 2: dùng duckdb.sql("COPY (SELECT * FROM read_csv(?, all_varchar=true, nullstr='')) TO ? (FORMAT parquet)")
    #   Gợi ý: DuckDB không nhận ? cho đường dẫn trong COPY, hãy format chuỗi SQL bằng f-string.
    raise NotImplementedError


def timed(con: duckdb.DuckDBPyConnection, sql: str, repeat: int = 5) -> tuple[list[tuple], float]:
    """Chạy `sql` `repeat` lần, trả về (kết quả, thời gian trung vị tính bằng ms)."""
    # TODO 3: đo bằng time.perf_counter() quanh con.execute(sql).fetchall(); lấy statistics.median
    raise NotImplementedError


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--source", default=str(REPO / "data" / "samples"), help="thư mục chứa pos_transactions.csv"
    )
    ap.add_argument("--out", default=str(REPO / "lake-local" / "v02"), help="nơi ghi file Parquet")
    args = ap.parse_args()

    csv_path = Path(args.source) / "pos_transactions.csv"
    parquet_path = Path(args.out) / "pos_transactions.parquet"
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    csv_to_parquet(csv_path, parquet_path)

    con = duckdb.connect()
    csv_src = f"read_csv('{csv_path}', all_varchar=true, nullstr='')"
    pq_src = f"read_parquet('{parquet_path}')"

    rows_csv, ms_csv = timed(con, REVENUE_SQL.format(src=csv_src))
    rows_pq, ms_pq = timed(con, REVENUE_SQL.format(src=pq_src))

    print(
        f"CSV     : {len(rows_csv):>5} dòng  {ms_csv:8.1f} ms  {csv_path.stat().st_size / 1024:8.0f} KB"
    )
    print(
        f"Parquet : {len(rows_pq):>5} dòng  {ms_pq:8.1f} ms  {parquet_path.stat().st_size / 1024:8.0f} KB"
    )
    # TODO 4: in tỉ lệ nhanh hơn (ms_csv / ms_pq) và tỉ lệ nhỏ hơn về dung lượng, 1 chữ số thập phân
    assert rows_csv == rows_pq, "hai cách lưu phải cho cùng kết quả"


if __name__ == "__main__":
    main()
