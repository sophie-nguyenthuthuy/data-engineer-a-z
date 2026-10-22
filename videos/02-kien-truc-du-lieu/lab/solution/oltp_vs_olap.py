"""Lời giải #02 — cùng một câu hỏi, hai cách lưu trữ (CSV row-oriented vs Parquet columnar, đều qua DuckDB).

Chạy:
    uv run python videos/02-kien-truc-du-lieu/lab/solution/oltp_vs_olap.py
    uv run python videos/02-kien-truc-du-lieu/lab/solution/oltp_vs_olap.py --source ./out
"""

from __future__ import annotations

import argparse
import statistics
import time
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[4]

REVENUE_SQL = """
SELECT
    substr(ts, 1, 10)            AS business_date,
    store_id,
    sum(total_amount_vnd::BIGINT) AS revenue_vnd
FROM {src}
WHERE store_id IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2
"""


def csv_to_parquet(csv_path: Path, parquet_path: Path) -> None:
    """CSV → Parquet, mọi cột VARCHAR (giống bronze: không đoán kiểu, typing là việc của silver)."""
    duckdb.sql(
        f"COPY (SELECT * FROM read_csv('{csv_path}', all_varchar=true, nullstr='')) "
        f"TO '{parquet_path}' (FORMAT parquet)"
    )


def timed(con: duckdb.DuckDBPyConnection, sql: str, repeat: int = 5) -> tuple[list[tuple], float]:
    """Chạy `repeat` lần, trả về (kết quả lần cuối, trung vị ms). Trung vị để bỏ lần đầu bị cold cache."""
    durations: list[float] = []
    rows: list[tuple] = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        rows = con.execute(sql).fetchall()
        durations.append((time.perf_counter() - t0) * 1000)
    return rows, statistics.median(durations)


def revenue_by_day_store(path: Path) -> list[tuple]:
    """Hàm tiện cho test: chạy câu hỏi nghiệp vụ trên 1 file (CSV hoặc Parquet)."""
    src = (
        f"read_parquet('{path}')"
        if path.suffix == ".parquet"
        else f"read_csv('{path}', all_varchar=true, nullstr='')"
    )
    return duckdb.sql(REVENUE_SQL.format(src=src)).fetchall()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=str(REPO / "data" / "samples"))
    ap.add_argument("--out", default=str(REPO / "lake-local" / "v02"))
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
    kb_csv = csv_path.stat().st_size / 1024
    kb_pq = parquet_path.stat().st_size / 1024

    print(f"CSV     : {len(rows_csv):>5} dòng  {ms_csv:8.1f} ms  {kb_csv:8.0f} KB")
    print(f"Parquet : {len(rows_pq):>5} dòng  {ms_pq:8.1f} ms  {kb_pq:8.0f} KB")
    print(f"Parquet nhanh hơn {ms_csv / ms_pq:.1f}x, nhỏ hơn {kb_csv / kb_pq:.1f}x")
    assert rows_csv == rows_pq, "hai cách lưu phải cho cùng kết quả"

    # Vì sao? Câu hỏi chỉ đụng 3 cột (ts, store_id, total_amount_vnd) trong 10 cột.
    # Parquet lưu theo cột → DuckDB chỉ đọc 3 cột đó, bỏ qua 7 cột còn lại ngay ở tầng I/O.
    # CSV là text theo dòng → phải đọc + parse đủ 10 cột của mọi dòng rồi mới bỏ.
    # Postgres (heap, row store) cũng vậy: mỗi page chứa đủ cột của vài chục dòng — xem lab/explain.sql.
    _, ms_all = timed(con, f"SELECT * FROM {pq_src}")
    print(
        f"Parquet SELECT * (đủ 10 cột, không aggregate): {ms_all:.1f} ms — lợi thế columnar biến mất"
    )


if __name__ == "__main__":
    main()
