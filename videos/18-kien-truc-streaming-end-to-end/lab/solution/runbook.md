# Runbook (lời giải) — chạy hệ thống streaming Phin Phố end-to-end

Chạy từ thư mục gốc repo. `host` = terminal máy bạn, `container` = `docker compose exec ...`. Mỗi mục: lệnh → lệnh kiểm chứng → "xong khi".

## 0. Khởi động
```bash
make up PROFILE=all
make ps
```
Kiểm chứng:
```bash
curl -s localhost:8083/connectors                       # [] → Debezium Connect sống
curl -s localhost:8085 | grep -c "Workers"              # Spark master UI trả HTML
docker compose exec minio mc ls local/lake/             # bronze/ silver/ gold/
```
Xong khi: `make ps` không còn `starting`/`unhealthy` (3–5 phút; Airflow, Trino lên sau cùng).

## 1. Topic
```bash
KT="docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:29092"
$KT --create --if-not-exists --topic pos.transactions     --partitions 3 --replication-factor 1
$KT --create --if-not-exists --topic iot.readings         --partitions 3 --replication-factor 1
$KT --create --if-not-exists --topic app.events           --partitions 3 --replication-factor 1
$KT --create --if-not-exists --topic pos.transactions.dlq --partitions 1 --replication-factor 1
$KT --list
```
Xong khi: `--list` có 4 topic trên (và các topic `_connect_*` của Connect).

## 2. Debezium
REPLICA IDENTITY đi trước vì pgoutput **từ chối** UPDATE/DELETE trên bảng publish mà không có replica identity — connector sẽ FAILED ngay khi có UPDATE đầu tiên.
```bash
docker compose exec postgres psql -U coffee -d coffee_pos \
  -c "ALTER TABLE pos_transactions REPLICA IDENTITY FULL;" -c "ALTER TABLE pos_line_items REPLICA IDENTITY FULL;"
curl -s -X POST -H "Content-Type: application/json" \
  --data @videos/16-cdc-debezium/lab/solution/register-connector.json localhost:8083/connectors | python -m json.tool
```
Kiểm chứng:
```bash
curl -s localhost:8083/connectors/coffee-pos/status | python -m json.tool          # "state": "RUNNING" ×2
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic coffee.public.pos_transactions --from-beginning --max-messages 1 | head -c 400   # envelope op "r"
docker compose exec postgres psql -U coffee -d coffee_pos -c "SELECT slot_name, active FROM pg_replication_slots;"   # dbz_coffee | t
```
Xong khi: Kafka UI (:8080) → `coffee.public.pos_transactions` có 2 481 message.

## 3. Spark Structured Streaming → Parquet gold (bật TRƯỚC producer)
```bash
docker compose exec -d spark-master spark-submit --master spark://spark-master:7077 \
  /opt/jobs/streaming_pos_agg.py --bootstrap kafka:29092 \
  --sink parquet --lake s3a://lake --checkpoint s3a://lake/_chk/pos_agg
```
(`-d` chạy nền; bỏ `-d` nếu muốn xem log trực tiếp trong terminal riêng.) Checkpoint trên S3: restart job đọc tiếp đúng offset Kafka và không ghi lại file đã commit.
Kiểm chứng:
```bash
open http://localhost:8085            # Running Applications: streaming_pos_agg
docker compose exec minio mc ls local/lake/_chk/pos_agg/      # offsets/ commits/ metadata xuất hiện sau Batch 0
```

## 4. Spark job cảnh báo IoT
```bash
docker compose cp videos/15-spark-structured-streaming/lab/solution/iot_anomaly_stream.py spark-master:/tmp/
docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
  --py-files /opt/jobs/common.py /tmp/iot_anomaly_stream.py --bootstrap kafka:29092 --watermark "2 minutes"
```
Terminal riêng; xong khi thấy `Batch: 0`.

## 5. Producer (3 terminal)
```bash
uv run --with confluent-kafka coffeegen stream --topic pos.transactions --start 2026-02-15 --scale 0.1 --rate 20
uv run --with confluent-kafka coffeegen stream --source iot --topic iot.readings --start 2026-02-16 --rate 100 --limit 20000
uv run --with confluent-kafka coffeegen stream --source app-events --topic app.events --start 2026-02-16 --scale 0.1 --rate 20
```
Kiểm chứng: Kafka UI → Messages count tăng ở 3 topic; terminal IoT job in cửa sổ `max_temp_c > 95.5`.
Xong khi: POS producer in `sent 1008 messages to pos.transactions`.

## 6. Consumer phụ trợ (2 terminal)
```bash
uv run --with confluent-kafka python videos/14-delivery-semantics-dlq-schema-registry/lab/solution/dlq_consumer.py --group e2e-dlq --from-beginning
uv run --with confluent-kafka python videos/16-cdc-debezium/lab/solution/cdc_apply.py --group e2e-cdc --db /tmp/cdc_pos.duckdb
```
Kiểm chứng:
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 \
  --topic pos.transactions.dlq --from-beginning --property print.headers=true --timeout-ms 5000 | wc -l   # 7 (null_store)
uv run python -c "import duckdb; print(duckdb.connect('/tmp/cdc_pos.duckdb', read_only=True).sql('SELECT count(*) FROM pos_transactions'))"  # 2476
```

## 7. Sink
```bash
docker compose exec minio mc ls local/lake/gold/rt_store_revenue_1m/          # part-*.parquet + _spark_metadata/
docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
  /opt/jobs/iceberg_silver.py --date 2026-02-15 --lake s3a://lake            # MERGE silver → Iceberg, in .snapshots
```
Kiểm chứng (host, DuckDB đọc MinIO):
```bash
uv run python -c "
import duckdb; c = duckdb.connect()
c.sql(\"INSTALL httpfs; LOAD httpfs; SET s3_endpoint='localhost:9000'; SET s3_use_ssl=false; SET s3_url_style='path'; SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin'\")
print(c.sql(\"SELECT count(*) windows, sum(orders) orders, sum(revenue_vnd) revenue FROM read_parquet('s3://lake/gold/rt_store_revenue_1m/*.parquet')\"))"
```
Xong khi: `orders` tiến gần 1 004 (1 008 − 4 duplicate đã `dropDuplicates`, trừ các cửa sổ cuối chưa đóng vì watermark) và `revenue` gần 96 234 000 (không tính 7 hoá đơn `store_id` null).

## 8. Đo độ trễ
```bash
docker compose exec jupyter python /home/jovyan/work/videos/18-kien-truc-streaming-end-to-end/lab/solution/latency_probe.py \
  --bootstrap kafka:29092 --s3-endpoint http://minio:9000 --duration 240
```
Chạy **trước** khi bật lại producer POS (tap chỉ thấy message mới). Ghi lại p50/p95 vào README của bạn.
<!-- TODO(thuy): điền p50/p95 đo thật -->

## 9. Dọn (đúng thứ tự)
```bash
curl -s -X DELETE localhost:8083/connectors/coffee-pos                                            # 1. connector
docker compose exec postgres psql -U coffee -d coffee_pos -c "SELECT pg_drop_replication_slot('dbz_coffee');"   # 2. slot — nếu quên, Postgres giữ WAL mãi
# 3. Ctrl+C các terminal spark-submit / producer / consumer
docker compose exec spark-master bash -c 'pkill -f streaming_pos_agg || true'                    #    job chạy nền (-d)
make down                                                                                        # 4. giữ volume; `make reset` nếu muốn sạch
```
Connector + slot trước vì chúng là thứ duy nhất gây hại khi quên: Kafka/Spark dừng thì chỉ mất độ trễ, slot bỏ quên thì **đầy đĩa Postgres**.
