"""Video #19 — backfill DAG: chạy lại ingest cho một khoảng ngày (params.start → params.end), tuần tự.

Trigger with config: {"start": "2026-02-14", "end": "2026-02-19"}
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param


@dag(
    dag_id="coffee_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    params={"start": Param("2026-02-14", type="string"), "end": Param("2026-02-19", type="string")},
    tags=["coffee", "manual"],
    doc_md=__doc__,
)
def coffee_backfill():
    @task
    def list_dates(params: dict | None = None) -> list[str]:
        start = pendulum.parse(params["start"]).date()
        end = pendulum.parse(params["end"]).date()
        return [str(start.add(days=i)) for i in range((end - start).days + 1)]

    @task(max_active_tis_per_dag=1)  # một ngày một lúc để không đè MinIO
    def ingest(run_date: str) -> str:
        import os
        import subprocess

        cmd = [
            "python",
            "-m",
            "coffee_ingestion.cli",
            "run",
            "--source",
            os.environ["POSTGRES_URL"],
            "--lake",
            "s3://lake",
            "--date",
            run_date,
        ]
        subprocess.run(
            cmd,
            cwd="/opt/ingestion",
            check=True,
            env={**os.environ, "S3_ENDPOINT": "http://minio:9000"},
        )
        return run_date

    ingest.expand(run_date=list_dates())


coffee_backfill()
