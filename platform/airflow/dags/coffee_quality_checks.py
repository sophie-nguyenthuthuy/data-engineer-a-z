"""Video #20 — DAG kiểm tra chất lượng, chạy khi silver pos_transactions được cập nhật (Dataset-triggered).

Hai lớp kiểm tra:
  1. Data contract (platform/quality/contracts/*.yaml) — schema + ràng buộc, check bằng code thuần.
  2. Great Expectations checkpoint (platform/quality/suites) — thống kê phân phối, null rate, freshness.
"""

from __future__ import annotations

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

SILVER_POS = Dataset("s3://lake/silver/pos_transactions")


@dag(
    dag_id="coffee_quality_checks",
    schedule=[SILVER_POS],
    start_date=pendulum.datetime(2026, 2, 14, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    tags=["coffee", "quality"],
    doc_md=__doc__,
)
def coffee_quality_checks():
    contract = BashOperator(
        task_id="contract_check",
        bash_command=(
            "cd /opt/quality && python contract_check.py "
            "--contract contracts/pos_transactions.yaml --parquet 's3://lake/silver/pos_transactions/dt={{ ds }}/*.parquet'"
        ),
        env={"S3_ENDPOINT": "http://minio:9000"},
        append_env=True,
    )
    gx = BashOperator(
        task_id="great_expectations_checkpoint",
        bash_command="cd /opt/quality && python run_checkpoint.py --date {{ ds }} --lake s3://lake",
        env={"S3_ENDPOINT": "http://minio:9000"},
        append_env=True,
    )

    @task
    def report() -> None:
        print("quality checks passed — xem Data Docs tại platform/quality/gx/uncommitted/data_docs")

    [contract, gx] >> report()


coffee_quality_checks()
