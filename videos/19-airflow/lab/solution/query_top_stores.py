"""Chạy đúng câu SQL của DAG coffee_weekly_report ở ngoài Airflow — để thử nhanh trước khi copy DAG vào.

    uv run python videos/19-airflow/lab/solution/query_top_stores.py --start 2026-02-14 --end 2026-02-18

Đọc file DuckDB mà `dbt build --target dev` tạo ở video #12 (platform/dbt/target/coffee_gold.duckdb).
"""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[4]
DEFAULT_DB = REPO / "platform" / "dbt" / "target" / "coffee_gold.duckdb"

TOP_STORES_SQL = """
select store_id, sum(revenue_vnd) as revenue_vnd, sum(transactions) as transactions
from main_gold.fct_daily_store_revenue
where business_date >= ? and business_date < ?
group by store_id
order by revenue_vnd desc
limit 5
"""


def top_stores(db_path: str | Path, start: dt.date, end: dt.date) -> list[dict]:
    """Top 5 cửa hàng theo doanh thu trong [start, end) — cùng SQL với DAG."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute(TOP_STORES_SQL, [start, end]).fetchall()
    finally:
        con.close()
    return [{"store_id": r[0], "revenue_vnd": int(r[1]), "transactions": int(r[2])} for r in rows]


def format_report(rows: list[dict], week_start: dt.date) -> str:
    lines = [f"Top 5 stores — week of {week_start}"]
    for i, r in enumerate(rows, start=1):
        revenue = f"{r['revenue_vnd']:,}".replace(",", ".")
        lines.append(f"{i}. {r['store_id']}  {revenue} VND  ({r['transactions']} hoá đơn)")
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db", default=str(DEFAULT_DB))
    p.add_argument("--start", required=True, help="YYYY-MM-DD, bao gồm")
    p.add_argument("--end", required=True, help="YYYY-MM-DD, KHÔNG bao gồm (như data_interval_end)")
    a = p.parse_args()
    start, end = dt.date.fromisoformat(a.start), dt.date.fromisoformat(a.end)
    print(format_report(top_stores(a.db, start, end), start))


if __name__ == "__main__":
    main()
