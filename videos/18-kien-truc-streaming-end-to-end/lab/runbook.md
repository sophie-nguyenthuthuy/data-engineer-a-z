# Runbook — chạy hệ thống streaming Phin Phố end-to-end (lab #18)

Điền **lệnh** và **lệnh kiểm chứng** cho từng bước. Mọi lệnh bạn đã gõ ở #13–#17; việc của bước này là xếp đúng thứ tự
và nói rõ "biết là xong khi thấy gì". Lời giải: `solution/runbook.md`.

Quy ước: chạy từ thư mục gốc repo; `host` = terminal máy bạn, `container` = `docker compose exec ...`.

## 0. Khởi động
Mục tiêu: mọi service `healthy`/`running`.
```bash
# TODO: lệnh up với profile đầy đủ + lệnh xem trạng thái
```
Kiểm chứng: `# TODO: 3 URL/lệnh bạn mở để tin là Kafka, Spark, MinIO đang sống`

## 1. Topic
Mục tiêu: `pos.transactions` (3 partition), `iot.readings` (3 partition), `pos.transactions.dlq` (1 partition) tồn tại trước khi có producer.
```bash
# TODO: kafka-topics.sh --create ×3 (trong container kafka, bootstrap localhost:29092)
```
Kiểm chứng: `# TODO: kafka-topics.sh --list`

## 2. Debezium (CDC từ Postgres)
Mục tiêu: connector `coffee-pos` RUNNING, topic `coffee.public.pos_transactions` có snapshot.
Thứ tự: REPLICA IDENTITY → đăng ký connector → kiểm tra status. Vì sao REPLICA IDENTITY phải đi trước?
```bash
# TODO
```
Kiểm chứng: `# TODO: curl status + đếm message snapshot (Kafka UI hoặc console consumer --max-messages)`

## 3. Spark Structured Streaming → Parquet gold
Mục tiêu: job `streaming_pos_agg.py` chạy với `--sink parquet`, checkpoint trên S3, **bật trước producer**.
```bash
# TODO: spark-submit ... --sink parquet --lake s3a://lake --checkpoint s3a://lake/_chk/pos_agg
```
Kiểm chứng: `# TODO: Spark UI có application RUNNING; log có "Batch: 0"`

## 4. Spark job cảnh báo IoT
Mục tiêu: `iot_anomaly_stream.py` (#15) chạy, console sink.
```bash
# TODO: docker compose cp + spark-submit --py-files /opt/jobs/common.py
```

## 5. Producer
Mục tiêu: ba luồng dữ liệu cùng chảy. Terminal riêng cho mỗi luồng.
```bash
# TODO: coffeegen stream POS (--rate 20), IoT (--source iot --rate 100 --limit 20000), app events (--source app-events --topic app.events)
```
Kiểm chứng: `# TODO: Kafka UI → message count tăng ở 3 topic; console Spark in Batch N có dòng`

## 6. Consumer phụ trợ
Mục tiêu: DLQ consumer (#14) và CDC apply (#16) chạy song song.
```bash
# TODO
```
Kiểm chứng: `# TODO: topic dlq có 7 message null_store; DuckDB cdc có ~2 476 dòng`

## 7. Sink
Mục tiêu: file Parquet xuất hiện ở `s3a://lake/gold/rt_store_revenue_1m/`; bảng Iceberg silver cập nhật.
```bash
# TODO: mc ls gold; spark-submit iceberg_silver.py
```
Kiểm chứng: `# TODO: DuckDB/Trino đọc được gold; .snapshots có thêm dòng`

## 8. Đo độ trễ
```bash
# TODO: latency_probe.py (trong jupyter hoặc host)
```
Ghi lại: p50 = ____ s, p95 = ____ s, với `--rate` = ____ và watermark = ____.

## 9. Dọn
Thứ tự quan trọng: connector + slot trước (vì sao?), rồi job, rồi `make down`.
```bash
# TODO
```
