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
    def business_date(data_interval_start=None) -> str:
        """Ngày nghiệp vụ = ngày (giờ VN) bắt đầu data interval.

        Bẫy hay gặp: `{{ ds }}` là ngày của logical_date tính theo **UTC**. Lịch 02:00 giờ VN = 19:00 UTC
        hôm trước, nên `ds` lệch đúng một ngày. Luôn đổi về Asia/Ho_Chi_Minh trước khi lấy ngày.
        """
        return data_interval_start.in_timezone(VN).to_date_string()

    ingest = BashOperator(
        task_id="ingest_bronze_silver",
        bash_command=(
            "cd /opt/ingestion && python -m coffee_ingestion.cli run "
            "--source ${POSTGRES_URL} --lake s3://lake --date {{ ti.xcom_pull(task_ids='business_date') }}"
        ),
        env={"S3_ENDPOINT": "{{ var.value.get('S3_ENDPOINT', 'http://minio:9000') }}"},
        append_env=True,
        outlets=[SILVER_POS],
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        # target-path riêng: dbt trong image (1.11) và dbt trên máy bạn (1.9) ghi layout target/ khác nhau,
        # dùng chung một thư mục qua bind mount sẽ lỗi "Not a directory" ở snapshot.
        bash_command=(
            "cd /opt/dbt && dbt build --profiles-dir . --target dev "
            "--target-path target_airflow --vars '{lake_root: s3://lake}'"
        ),
        env={
            "DUCKDB_S3_ENDPOINT": "minio:9000",
            "DBT_DUCKDB_PATH": "/opt/dbt/target_airflow/coffee_gold.duckdb",
        },
        append_env=True,
        outlets=[GOLD],
    )

    @task
    def notify(summary_date: str) -> None:
        """Báo xong một ngày. Webhook lấy từ Airflow Variable (Admin → Variables), không có thì chỉ in log.

        - Slack:    Variable `SLACK_WEBHOOK_URL`  = https://hooks.slack.com/services/T000/B000/XXXX
        - Telegram: Variable `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`
        Chỉ dùng stdlib (urllib) để DAG không phụ thuộc provider nào; lỗi gửi tin **không** làm task fail —
        pipeline đã xong, thông báo là việc phụ.
        """
        import json
        import urllib.request

        from airflow.models import Variable

        text = f"✅ coffee_daily_pipeline xong cho ngày {summary_date}"
        print(text)
        slack = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
        tg_token = Variable.get("TELEGRAM_BOT_TOKEN", default_var=None)
        tg_chat = Variable.get("TELEGRAM_CHAT_ID", default_var=None)
        targets = []
        if slack:
            targets.append((slack, {"text": text}))
        if tg_token and tg_chat:
            targets.append(
                (
                    f"https://api.telegram.org/bot{tg_token}/sendMessage",
                    {"chat_id": tg_chat, "text": text},
                )
            )
        if not targets:
            print("no SLACK_WEBHOOK_URL / TELEGRAM_* Variable set → log only")
        for url, payload in targets:
            req = urllib.request.Request(
                url, data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}
            )
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    print(f"notified {url.split('/')[2]} → HTTP {resp.status}")
            except Exception as exc:  # noqa: BLE001 — thông báo lỗi không được làm pipeline đỏ
                print(f"notify failed for {url.split('/')[2]}: {exc}")

    bd = business_date()
    bd >> ingest >> dbt_build >> notify(bd)


coffee_daily_pipeline()
