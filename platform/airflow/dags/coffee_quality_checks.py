"""Video #20 — DAG kiểm tra chất lượng, chạy khi silver pos_transactions được cập nhật (Dataset-triggered).

Hai lớp kiểm tra:
  1. Data contract (platform/quality/contracts/*.yaml) — schema + ràng buộc, check bằng code thuần.
  2. Great Expectations checkpoint (platform/quality/suites) — thống kê phân phối, null rate, freshness.
"""

from __future__ import annotations

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

SILVER_POS = Dataset("s3://lake/silver/pos_transactions")


@dag(
    dag_id="coffee_quality_checks",
    schedule=[SILVER_POS],
    start_date=pendulum.datetime(2026, 2, 14, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    params={"date": Param(None, type=["null", "string"], description="YYYY-MM-DD khi trigger tay")},
    tags=["coffee", "quality"],
    doc_md=__doc__,
)
def coffee_quality_checks():
    @task
    def business_date(triggering_dataset_events=None, params=None, data_interval_start=None) -> str:
        """Ngày cần kiểm tra.

        Run do Dataset kích hoạt không có "ngày nghiệp vụ" riêng (logical_date = lúc được kích hoạt), nên
        lấy ngày từ run upstream: XCom `business_date` của `coffee_daily_pipeline`. Trigger tay → params.date,
        không có thì ngày (giờ VN) của data interval.
        """
        from airflow.models import XCom

        for events in (triggering_dataset_events or {}).values():
            for ev in events:
                if ev.source_dag_id == "coffee_daily_pipeline":
                    found = XCom.get_one(
                        run_id=ev.source_run_id, dag_id=ev.source_dag_id, task_id="business_date"
                    )
                    if found:
                        return found
        if params and params.get("date"):
            return params["date"]
        return data_interval_start.in_timezone("Asia/Ho_Chi_Minh").to_date_string()

    contract = BashOperator(
        task_id="contract_check",
        bash_command=(
            "cd /opt/quality && python contract_check.py "
            "--contract contracts/pos_transactions.yaml "
            "--parquet 's3://lake/silver/pos_transactions/dt={{ ti.xcom_pull(task_ids='business_date') }}/*.parquet'"
        ),
        env={"S3_ENDPOINT": "http://minio:9000"},
        append_env=True,
    )
    gx = BashOperator(
        task_id="great_expectations_checkpoint",
        bash_command=(
            "cd /opt/quality && python run_checkpoint.py "
            "--date {{ ti.xcom_pull(task_ids='business_date') }} --lake s3://lake"
        ),
        env={"S3_ENDPOINT": "http://minio:9000"},
        append_env=True,
    )

    @task
    def report() -> None:
        print("quality checks passed — xem Data Docs tại platform/quality/gx/uncommitted/data_docs")

    business_date() >> [contract, gx] >> report()


coffee_quality_checks()
