# platform/airflow — orchestration (video #19, #20)

`make up PROFILE=all` → http://localhost:8082 (admin/admin). DAG mount từ `dags/`, sửa file là Airflow reload sau ~30 s.

| DAG | Lịch | Việc |
|-----|------|------|
| `coffee_daily_pipeline` | 02:00 giờ VN hằng ngày | ingest (bronze→silver) → dbt build → data quality → notify |
| `coffee_backfill` | manual | chạy lại pipeline cho khoảng ngày (params start/end) |
| `coffee_quality_checks` | sau `coffee_daily_pipeline` (Dataset-triggered) | Great Expectations checkpoint + data contract check, fail → ghi Marquez facet |

Nguyên tắc trong DAG: task **idempotent**, dùng `{{ ds }}` làm business date, không hard-code hôm nay; retries=2; SLA 1 giờ.
