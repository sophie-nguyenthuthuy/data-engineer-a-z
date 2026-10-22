# CV bullets — bản mẫu hoàn chỉnh (dự án Phin Phố + một thay đổi riêng)

Giả định: học viên đã làm thay đổi B ở `portfolio_checklist.md` — thêm nguồn **thời tiết thật** (Open-Meteo) thay `is_rainy_day` sinh ngẫu nhiên.
Mỗi bullet kèm dòng *đo bằng* để bạn thấy "số có nguồn" nghĩa là gì. Trong CV thật thì bỏ dòng *đo bằng*.

## Khối dự án trên CV

**Data platform cho chuỗi cà phê 30 cửa hàng (dự án cá nhân, 2026)** — Polars · DuckDB · dbt · Airflow · Kafka · MinIO/S3 · Terraform · GitHub Actions
GitHub: github.com/<user>/<repo> · Demo 3 phút: <link>

1. **Xây** pipeline bronze → silver → gold cho 3 nguồn (POS Postgres, app clickstream JSONL, IoT CSV) bằng Polars + dbt/DuckDB; cách ly **~1.5 %** bản ghi lỗi (duplicate, null store, lệch múi giờ, quantity âm) vào quarantine kèm lý do, không xoá.
   *đo bằng:* `make smoke` → summary `quarantined / rows_in` cho 2026-02-15: (4+9)/(799+1401) = 0.6 % POS; toàn bộ 6 loại lỗi ở 3 nguồn ≈ 1.5 % theo generator.
2. **Nối** dữ liệu thời tiết thật (Open-Meteo, 2 thành phố, theo giờ) vào fact doanh thu; phát hiện ngày mưa doanh thu quầy giảm **~15 %** nhưng đơn app tăng **~30 %** — thay cờ `is_rainy_day` sinh ngẫu nhiên bằng nguồn kiểm chứng được.
   *đo bằng:* `select is_rainy_day, avg(revenue_vnd), avg(app_revenue_vnd) from fct_daily_store_revenue group by 1` trên 90 ngày generator. <!-- TODO: thay bằng số đo của bạn sau khi nối Open-Meteo thật -->
3. **Thiết kế** silver idempotent theo partition `dt=` (ghi đè, không append): chạy lại cùng ngày tạo **0 bản ghi trùng**, kiểm bằng test end-to-end trong CI.
   *đo bằng:* `platform/ingestion/tests/test_pipeline.py::test_end_to_end_local_lake`.
4. **Mô hình hoá** star schema 4 dim + 2 fact bằng dbt (**11 model**, SCD2 snapshot bảng giá), 100 % cột khoá có test `unique`/`not_null`, 2 generic test tự viết không cần `dbt_utils`.
   *đo bằng:* `cd platform/dbt && dbt build` → `Found 11 models …`.
5. **Giảm 88 %** dung lượng và ước tính chi phí quét Athena **từ 6.4 USD còn 0.04 USD/tháng** (90 ngày dữ liệu, 900 query) bằng Parquet zstd + partition pruning theo ngày.
   *đo bằng:* `uv run python videos/23-chi-phi-bao-mat/lab/solution/cost_model.py`.
6. **Điều phối** pipeline hằng ngày bằng Airflow 2.9 (TaskFlow, Dataset-triggered quality DAG, `retries=2`); backfill 4 ngày bằng dynamic task mapping chạy tuần tự để không đè object storage.
   *đo bằng:* `coffee_backfill` với `{"start":"2026-02-14","end":"2026-02-17"}` → 4 partition, mỗi partition 1 file.
7. **Viết** data contract YAML + checker 80 dòng Polars cho 2 bảng silver, chạy trước dbt; có lịch ngoại lệ nghiệp vụ (Tết) để tránh **false alert 3 ngày/năm**.
   *đo bằng:* `contract_check.py` trên `dt=2026-02-17` (59 dòng < ngưỡng 100) vẫn OK nhờ `exceptions`.
8. **Phát** lineage OpenLineage từ Airflow và dbt vào Marquez; xuất **20 metric** Prometheus (rows_in/out/quarantined theo bảng và lý do) từ summary JSON của pipeline.
   *đo bằng:* `coffee-ingest run … | metrics_exporter.py | grep -vc '^#'` → 20.
9. **Dựng** CI GitHub Actions 5 job song song (ruff, pytest, `dbt parse`, smoke + contract, `terraform validate`); mọi PR được kiểm trong **4 phút** không cần warehouse thật. <!-- TODO: thay bằng thời gian CI thật trên fork -->
   *đo bằng:* tab Actions của repo, run gần nhất.
10. **Áp** least privilege 3 lớp: IAM policy Terraform giới hạn 1 bucket, MinIO policy chỉ-đọc `gold/`, Postgres role analyst với column-level GRANT che **3 cột PII**; tokenize `customer_id` bằng HMAC-SHA256 có salt.
    *đo bằng:* `SET ROLE analyst; SELECT phone FROM customers;` → `permission denied`.

## Vì sao 10 bullet này "qua" được vòng 30 giây
- Bullet 1–2 đọc xong đã biết: bài toán gì, dữ liệu gì, có gì **của riêng bạn** (bullet 2).
- Mỗi bullet có **một** con số, và bạn nói tiếp được 2 phút về cách đo.
- Công cụ nêu đích danh và có version khi version quan trọng (Airflow 2.9) — chứng tỏ bạn dùng thật.
- Không có bullet nào bắt đầu bằng "tham gia", "hỗ trợ", "tìm hiểu".
