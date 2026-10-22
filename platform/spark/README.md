# platform/spark — PySpark jobs

Chạy trên cluster trong docker-compose (`make up PROFILE=spark`):

```bash
docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
  /opt/jobs/bronze_to_silver_pos.py --date 2026-02-15 --lake s3a://lake
```

hoặc local trong Jupyter (`SparkSession.builder.master("local[*]")`) — mọi job đều nhận `--lake` là đường dẫn local hay `s3a://`.

| Job | Video | Việc |
|-----|-------|------|
| `common.py` | — | tạo SparkSession, đọc tham số, helper đường dẫn |
| `bronze_to_silver_pos.py` | #10 | phiên bản Spark của pipeline Python ở video #07 (so sánh 2 cách) |
| `skew_demo.py` | #11 | tái hiện data skew (SG01 chiếm 40% dòng) → sửa bằng salting / broadcast / AQE |
| `streaming_pos_agg.py` | #15 | Structured Streaming từ Kafka `pos.transactions`: doanh thu theo cửa hàng mỗi 1 phút, watermark 10 phút |
| `iceberg_silver.py` | #17 | ghi silver thành Iceberg table, MERGE INTO (upsert), time travel |

<!-- TODO(thuy): chạy thử từng job trên cluster compose và dán output thật vào README video tương ứng. Code đã lint nhưng chưa chạy end-to-end trên cluster. -->
