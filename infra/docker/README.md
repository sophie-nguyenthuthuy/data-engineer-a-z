# infra/docker

Dockerfile tuỳ biến cho các image không có sẵn đúng thứ mình cần:

| File | Dùng cho | Thêm gì |
|------|----------|---------|
| `spark.Dockerfile` | `spark-master`, `spark-worker` | base `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` (chính thức; Bitnami đã gỡ image miễn phí) + Iceberg runtime, hadoop-aws, spark-sql-kafka, driver postgres (JDBC catalog) |
| `jupyter.Dockerfile` | `jupyter` | DuckDB, Polars, psycopg, confluent-kafka, Great Expectations + cùng bộ jar S3A/Iceberg |
| `airflow.Dockerfile` | `airflow-*` | providers postgres/amazon/spark/openlineage theo constraints 2.9.3; dbt, GX, psycopg, pytest; `pyarrow<19` vì numpy 1.26 |
| `trino/catalog/` | `trino` | catalog `lake` (Iceberg, JDBC catalog trong Postgres, dữ liệu trên MinIO) và `postgres` |

MinIO dùng `bitnamilegacy/minio:2024.10.13` (MinIO cũng đã gỡ image khỏi Docker Hub/quay); image này có sẵn `mc` nên `docker compose exec minio mc …` chạy được, và `MINIO_DEFAULT_BUCKETS` tạo bucket lúc khởi động.

Build lần đầu mất ~5–10 phút (tải jar + pip). Image được cache, lần sau `make up` lên trong vài giây. Đã build và chạy end-to-end trên macOS arm64 (Docker Desktop); Linux x86_64 dùng cùng tag (multi-arch).
