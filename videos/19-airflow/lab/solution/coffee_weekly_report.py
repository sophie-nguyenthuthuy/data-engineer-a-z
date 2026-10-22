"""Lời giải lab #19 — DAG báo cáo tuần: top 5 cửa hàng theo doanh thu, đọc từ gold DuckDB do dbt tạo.

Đưa vào Airflow:  cp videos/19-airflow/lab/solution/coffee_weekly_report.py platform/airflow/dags/
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

VN = pendulum.timezone("Asia/Ho_Chi_Minh")
GOLD_DB = "/opt/dbt/target/coffee_gold.duckdb"

TOP_STORES_SQL = """
select store_id, sum(revenue_vnd) as revenue_vnd, sum(transactions) as transactions
from main_gold.fct_daily_store_revenue
where business_date >= ? and business_date < ?
group by store_id
order by revenue_vnd desc
limit 5
"""


@dag(
    dag_id="coffee_weekly_report",
    schedule="0 3 * * 1",  # thứ Hai 03:00 giờ VN, sau coffee_daily_pipeline (02:00)
    start_date=pendulum.datetime(2026, 2, 9, tz=VN),
    catchup=False,
    tags=["coffee", "weekly"],
    doc_md=__doc__,
)
def coffee_weekly_report():
    @task
    def top_stores(data_interval_start=None, data_interval_end=None) -> list[dict]:
        import duckdb

        start = data_interval_start.in_tz(VN).date()
        end = data_interval_end.in_tz(VN).date()
        con = duckdb.connect(GOLD_DB, read_only=True)
        try:
            rows = con.execute(TOP_STORES_SQL, [start, end]).fetchall()
        finally:
            con.close()
        # XCom mặc định là JSON → ép HUGEINT của DuckDB về int thường
        return [
            {"store_id": r[0], "revenue_vnd": int(r[1]), "transactions": int(r[2])} for r in rows
        ]

    @task
    def print_report(rows: list[dict], data_interval_start=None) -> None:
        week = data_interval_start.in_tz(VN).date()
        print(f"Top 5 stores — week of {week}")
        for i, r in enumerate(rows, start=1):
            revenue = f"{r['revenue_vnd']:,}".replace(",", ".")
            print(f"{i}. {r['store_id']}  {revenue} VND  ({r['transactions']} hoá đơn)")
        if not rows:
            print("(gold trống cho tuần này — chạy coffee_backfill trước)")

    print_report(top_stores())


coffee_weekly_report()
