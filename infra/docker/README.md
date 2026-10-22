# infra/docker

Dockerfile tuỳ biến cho các image không có sẵn đúng thứ mình cần:

| File | Dùng cho | Thêm gì |
|------|----------|---------|
| `spark.Dockerfile` | `spark-master`, `spark-worker` | Iceberg runtime, hadoop-aws, spark-sql-kafka |
| `jupyter.Dockerfile` | `jupyter` | DuckDB, Polars, psycopg, confluent-kafka, Great Expectations + cùng bộ jar S3A/Iceberg |
| `airflow.Dockerfile` | `airflow-*` | providers postgres/amazon/spark/openlineage, dbt, GX |
| `trino/catalog/` | `trino` | catalog `lake` (Iceberg trên MinIO) và `postgres` |

Build lần đầu mất ~5–10 phút (tải jar). Image được cache, lần sau `make up` lên trong vài giây.
