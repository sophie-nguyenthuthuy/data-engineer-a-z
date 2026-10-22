"""Lab #19 — DAG báo cáo tuần: top 5 cửa hàng theo doanh thu, đọc từ gold DuckDB do dbt tạo.

Chạy sau `coffee_daily_pipeline` của sáng thứ Hai. Chỉ dùng @task Python + duckdb (có sẵn trong image
Airflow của repo), không cần provider.

Đưa vào Airflow:  cp videos/19-airflow/lab/dags/coffee_weekly_report.py platform/airflow/dags/
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

VN = pendulum.timezone("Asia/Ho_Chi_Minh")
# dbt_build trong coffee_daily_pipeline ghi vào đây (DBT_DUCKDB_PATH). Schema gold của dbt-duckdb là main_gold.
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
    # TODO: cron chạy thứ Hai 03:00 giờ VN (sau coffee_daily_pipeline lúc 02:00).
    #       Gợi ý: 5 trường "phút giờ ngày tháng thứ"; thứ Hai = 1.
    schedule=None,
    start_date=pendulum.datetime(2026, 2, 9, tz=VN),  # một thứ Hai
    catchup=False,
    tags=["coffee", "weekly"],
    doc_md=__doc__,
)
def coffee_weekly_report():
    @task
    def top_stores(data_interval_start=None, data_interval_end=None) -> list[dict]:
        """Top 5 cửa hàng trong tuần [data_interval_start, data_interval_end).

        Airflow tự truyền hai tham số này theo tên (TaskFlow context). Với schedule tuần,
        data interval = đúng 7 ngày của tuần trước — KHÔNG dùng datetime.now().
        """
        import duckdb  # noqa: F401 — dùng ở TODO bên dưới

        # TODO:
        #   1. start = data_interval_start.in_tz(VN).date(); end = data_interval_end.in_tz(VN).date()
        #   2. con = duckdb.connect(GOLD_DB, read_only=True); rows = con.execute(TOP_STORES_SQL, [start, end]).fetchall()
        #   3. trả về list[dict] — XCom là JSON nên ép revenue_vnd/transactions về int()
        raise NotImplementedError

    @task
    def print_report(rows: list[dict], data_interval_start=None) -> None:
        # TODO: in tiêu đề "Top 5 stores — week of <ngày bắt đầu>" rồi mỗi dòng
        #       "<thứ hạng>. <store_id>  <revenue_vnd định dạng có dấu chấm>  (<transactions> hoá đơn)"
        raise NotImplementedError

    print_report(top_stores())


coffee_weekly_report()
