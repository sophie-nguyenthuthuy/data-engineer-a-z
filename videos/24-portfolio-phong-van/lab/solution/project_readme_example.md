# Phin Phố Data Platform — doanh thu & vận hành cho chuỗi 30 cửa hàng cà phê, đúng trước 8h sáng

<!-- Bản mẫu README dự án điền theo khung project_readme_template.md. Số liệu lấy từ repo này (data/samples, ngày chuẩn 2026-02-15). Đọc để biết "đủ" là bao nhiêu; dự án của bạn phải có số của bạn. -->

[![CI](https://github.com/<user>/<repo>/actions/workflows/ci.yml/badge.svg)](https://github.com/<user>/<repo>/actions) · 📺 [Demo 3 phút](TODO:link)

## 1. Problem
Chuỗi **Phin Phố** có 30 cửa hàng (15 quận Hà Nội, 15 quận TP.HCM) và 3 hệ thống rời: POS tại quầy (Postgres), app đặt hàng (clickstream JSONL), cảm biến máy pha espresso (CSV). Quản lý vùng cần doanh thu theo ngày × cửa hàng trước 8h sáng; hiện tổng hợp tay từ export POS, sai lệch vài % vì **hoá đơn trùng do retry**, **timestamp thiếu múi giờ** làm doanh thu nhảy ngày, và máy POS mới chưa cấu hình `store_id`. Không ai biết máy pha nào sắp hỏng.

## 2. Architecture
![architecture](docs/architecture.png) <!-- TODO: vẽ bằng excalidraw: 3 nguồn → bronze/silver (Polars) → gold (dbt/DuckDB) → BI; Airflow điều phối; Kafka/Debezium cho CDC; Marquez lineage -->

| Lớp | Công nghệ | Vì sao |
|-----|-----------|--------|
| Nguồn | Postgres 16 (POS), JSONL (app), CSV (IoT) | mô phỏng đúng 3 dạng nguồn một chuỗi F&B thật có |
| Ingest | Polars CLI `coffee-ingest`, Debezium → Kafka cho CDC | batch đủ cho báo cáo ngày; CDC chỉ cho metric vận hành |
| Lưu trữ | MinIO/S3, Parquet zstd, partition `dt=` | rẻ, mở, đọc được bằng mọi engine; pruning theo ngày |
| Transform | dbt + DuckDB (dev) / Trino (analytics) | SQL review được, test đi kèm model |
| Orchestration | Airflow 2.9 | TaskFlow + Dataset; backfill có kiểm soát |
| Quality / Observability | contract YAML, dbt test, GX; OpenLineage → Marquez; metric Prometheus text | 3 lớp từ rẻ tới đắt, mỗi lớp bắt một loại lỗi |

## 3. Data
- POS: ~770 hoá đơn/ngày (mẫu, scale 0.08) ≈ **9 600/ngày** ở quy mô thật; ~1.75 món/hoá đơn. App: ~2 000 event/ngày mẫu. IoT: 60 máy × 1 reading/phút. Schema đầy đủ: [data/generator/SCHEMA.md](data/generator/SCHEMA.md).
- Lỗi dữ liệu (cố ý trong generator, mô phỏng lỗi thật) và cách xử lý:

| Lỗi | Tỉ lệ (15/02) | Xử lý ở lớp | Cách |
|-----|---------------|-------------|------|
| duplicate | 3/799 | silver | giữ bản sớm nhất theo `transaction_id`, ưu tiên có `store_id` |
| null_store | 4/799 | silver | sang `quarantine_pos_transactions`, lý do `null_store` |
| tz_drift | 5/799 | silver | thiếu offset ⇒ hiểu là +07:00 → lưu UTC |
| negative_qty | 3/1401 | silver | line item sang quarantine |
| orphan line | 6/1401 | silver | line không có hoá đơn hợp lệ → quarantine |
| late_event | ~1 % app/IoT | streaming | watermark 10 phút; batch đêm tính lại số chính thức |

## 4. Pipeline
```bash
make install && make up                       # postgres + minio + jupyter, ~5 phút lần đầu
make smoke                                    # bronze + silver ngày 2026-02-15 → ./lake-local, < 1 s
cd platform/dbt && uv run dbt build --profiles-dir . --target dev   # 11 model + test, ~20 s
```
- Lịch: Airflow `coffee_daily_pipeline` 02:00 giờ VN, `retries=2`, `max_active_runs=1`; `coffee_quality_checks` chạy theo Dataset khi silver cập nhật; `coffee_backfill` nhận `{"start","end"}`.
- Idempotency: partition `dt=` ghi đè; chạy lại không tạo bản ghi trùng — `test_end_to_end_local_lake` chạy pipeline 2 lần và so số dòng.
- Streaming (chỉ cho vận hành): Debezium → Kafka → Spark Structured Streaming tính đơn/phút và cảnh báo máy pha `status = error`.

## 5. Quality
- Data contract: `platform/quality/contracts/*.yaml` (2 bảng silver, 17 ràng buộc), checker 80 dòng Polars, chạy trong Airflow và CI; ngoại lệ Tết để không alert giả.
- Tests: 30+ pytest (generator, ingestion, lab) chạy trong CI < 2 phút; dbt test 100 % cột khoá; GX suite 8 expectation cho `silver.pos_transactions`. <!-- TODO: cập nhật số test thật -->
- Metrics / lineage: 20 metric Prometheus (`rows_in/out/quarantined` theo bảng, lý do) từ summary JSON; lineage Airflow + dbt vào Marquez.
- Một lần quality bắt được lỗi thật: khi đổi dedup từ `ORDER BY ts` sang ưu tiên `store_id IS NOT NULL`, test `unique transaction_id` ở dbt vẫn pass nhưng contract báo `customer_id: 2 values do not match ^C[0-9]{6}$` — hoá ra bản sao được giữ lại có `customer_id` rác. Sửa dedup, thêm test cho case đó.

## 6. Lessons
1. **Bronze phải ngu**: không sửa gì ở bronze giúp mình re-process silver 4 lần khi đổi rule mà không cần hỏi lại nguồn.
2. Ngưỡng cứng cần lịch ngoại lệ: `min_rows_per_day` báo động 3 ngày Tết trước khi mình thêm `exceptions`.
3. Điều làm sai: DAG đầu tiên dùng `datetime.now()` — backfill chạy 4 ngày đều nạp hôm nay. Giờ có test `ast` cấm `now()` trong mọi file DAG.

## Việc tiếp theo
- [ ] Nối thời tiết thật (Open-Meteo) thay cờ `is_rainy_day`.
- [ ] Iceberg cho silver để có time travel thay vì backfill từ bronze.
- [ ] Alert Telegram từ Airflow `on_failure_callback`.

---
<sub>Khởi đầu từ khoá học "Data Engineer từ A–Z" (Nguyễn Thu Thuỷ, CC BY-NC-SA 4.0 cho nội dung, MIT cho code). Những thay đổi của mình: thêm nguồn thời tiết, mô hình chi phí, metric exporter.</sub>
