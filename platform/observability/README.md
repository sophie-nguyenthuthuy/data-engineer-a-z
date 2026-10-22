# platform/observability — Lineage & Observability (video #21)

Chuẩn **OpenLineage**: mỗi job (Airflow task, Spark job, dbt run) gửi event START/COMPLETE kèm input/output dataset tới **Marquez** (http://localhost:3000).

| File | Dùng cho |
|------|----------|
| `marquez.yml` | config Marquez API (đã mount vào compose) |
| `openlineage-spark.conf` | thêm vào `spark-submit --properties-file` để Spark job tự gửi lineage |
| `openlineage-dbt.yml` | `dbt-ol run` thay cho `dbt run` → lineage model-level |
| `emit_lineage.py` | gửi event tay bằng HTTP — để hiểu format trước khi dùng integration |

Airflow đã bật provider openlineage qua biến `AIRFLOW__OPENLINEAGE__TRANSPORT` trong docker-compose.
