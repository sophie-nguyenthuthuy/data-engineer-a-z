"""Video #19 — DAG hằng ngày: source → bronze → silver → dbt → quality.

Mỗi task là một lệnh CLI của repo, nên bạn chạy tay được từng bước y hệt Airflow chạy.
"""

from __future__ import annotations

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

VN = pendulum.timezone("Asia/Ho_Chi_Minh")
SILVER_POS = Dataset("s3://lake/silver/pos_transactions")
GOLD = Dataset("duckdb://coffee_gold/gold")

DEFAULT_ARGS = {"owner": "data-platform", "retries": 2, "retry_delay": pendulum.duration(minutes=5)}


@dag(
    dag_id="coffee_daily_pipeline",
    schedule="0 2 * * *",  # 02:00 giờ VN — POS đã đóng ca, dữ liệu hôm trước đã đủ
    start_date=pendulum.datetime(2026, 2, 14, tz=VN),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["coffee", "daily"],
    doc_md=__doc__,
)
def coffee_daily_pipeline():
    @task
    def business_date(ds: str | None = None) -> str:
        """Airflow's `ds` is the start of the data interval = the day we are loading."""
        return ds  # type: ignore[return-value]

    ingest = BashOperator(
        task_id="ingest_bronze_silver",
        bash_command=(
            "cd /opt/ingestion && python -m coffee_ingestion.cli run "
            "--source ${POSTGRES_URL} --lake s3://lake --date {{ ds }}"
        ),
        env={"S3_ENDPOINT": "{{ var.value.get('S3_ENDPOINT', 'http://minio:9000') }}"},
        append_env=True,
        outlets=[SILVER_POS],
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/dbt && dbt build --profiles-dir . --target dev --vars '{lake_root: s3://lake}'",
        env={
            "DUCKDB_S3_ENDPOINT": "minio:9000",
            "DBT_DUCKDB_PATH": "/opt/dbt/target/coffee_gold.duckdb",
        },
        append_env=True,
        outlets=[GOLD],
    )

    @task
    def notify(summary_date: str) -> None:
        # TODO(thuy): nối Slack/Telegram webhook ở video #19 phần cuối
        print(f"✅ coffee_daily_pipeline done for {summary_date}")

    bd = business_date()
    bd >> ingest >> dbt_build >> notify(bd)


coffee_daily_pipeline()
