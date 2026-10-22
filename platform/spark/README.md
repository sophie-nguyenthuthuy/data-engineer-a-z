# platform/spark — PySpark jobs

Chạy trên cluster trong docker-compose (`make up PROFILE=spark`):

```bash
docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
  /opt/jobs/bronze_to_silver_pos.py --date 2026-02-15 --lake s3a://lake
```

Trong container: `platform/spark` → `/opt/jobs`, `platform/lake` → `/opt/lake`, `videos/` → `/opt/videos` (chạy lab trực tiếp: `spark-submit --py-files /opt/jobs/common.py /opt/videos/10-apache-spark/lab/wordcount_to_revenue.py`), `data/samples` → `/opt/samples`.

hoặc local trong Jupyter (`SparkSession.builder.master("local[*]")`) — mọi job đều nhận `--lake` là đường dẫn local hay `s3a://`.

| Job | Video | Việc |
|-----|-------|------|
| `common.py` | — | tạo SparkSession, đọc tham số, helper đường dẫn |
| `bronze_to_silver_pos.py` | #10 | phiên bản Spark của pipeline Python ở video #07 (so sánh 2 cách) |
| `skew_demo.py` | #11 | tái hiện data skew (SG01 chiếm 40% dòng) → sửa bằng salting / broadcast / AQE |
| `streaming_pos_agg.py` | #15 | Structured Streaming từ Kafka `pos.transactions`: doanh thu theo cửa hàng mỗi 1 phút, watermark 10 phút |
| `iceberg_silver.py` | #17 | ghi silver thành Iceberg table, MERGE INTO (upsert), time travel |

Cả 4 job đã chạy end-to-end trên cluster compose (output thật trong README video #10, #11, #15, #17). `skew_demo.py` thêm `--conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events` nếu muốn đọc số đo task sau khi job xong (Spark UI của application biến mất khi job kết thúc).
