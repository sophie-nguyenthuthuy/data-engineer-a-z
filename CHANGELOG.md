# Changelog

Định dạng theo [Keep a Changelog](https://keepachangelog.com/vi/1.0.0/), commit theo Conventional Commits.

## [0.3.0] — 2026-10-22 — Slide cho 24 video

### Added
- `videos/NN-*/slides/`: deck PPTX (14–16 slide mỗi video, 8,9 MB tổng) + PDF xuất bằng LibreOffice headless (3,8 MB tổng) cho #01–#24; link 📑 Slide ngay dưới dòng 📺 Video trong README từng video. #00 intro không có slide (video nói chuyện trực tiếp).

## [0.2.1] — 2026-10-22 — Ảnh UI thật, Spark History Server

### Added
- Ảnh chụp thật (Playwright, chromium headless) đưa vào README: Spark History Server / Jobs / SQL DAG (#10), Stages + Stage detail naive vs aqe (#11), Airflow Graph + Grid (#19), Marquez lineage manual + dbt (#21). `videos/NN-*/img/`, mỗi ảnh ~100–250 KB.
- `spark-history` service (profile `spark`, http://localhost:18080) + `spark.eventLog.enabled` mặc định trong image; port 4040 của `spark-master` publish để mở Application UI của job đang chạy. Lý do: UI của application biến mất khi job xong, link từ Master UI trỏ vào hostname trong container.

### Checked
- Mọi image đều có `linux/amd64` + `linux/arm64`, trừ `marquezproject/marquez(-web):0.49.0` chỉ amd64 (Mac chạy qua Rosetta, đã chạy trọn #21). Chưa chạy thật trên Linux x86_64/Windows.

## [0.2.0] — 2026-10-22 — Chạy end-to-end trên Docker, dán output thật

Toàn bộ môi trường đã được dựng thật (`make up PROFILE=all`, macOS arm64, Docker Desktop) và mọi lab có bước Docker đã chạy
một lượt. Những gì sửa dưới đây đều là lỗi lộ ra khi chạy thật, không phải khi lint.

### Changed — hạ tầng
- **Spark image**: `bitnami/spark:3.5.3` đã bị gỡ khỏi Docker Hub → chuyển sang `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` (chính thức, Python 3.10). Compose đổi `SPARK_MODE` sang lệnh `spark-class … Master/Worker`; `spark-submit` có trên PATH.
- **MinIO image**: `minio/minio` và `minio/mc` cũng đã bị gỡ (Docker Hub lẫn quay.io) → `bitnamilegacy/minio:2024.10.13` (có sẵn `mc`, `MINIO_DEFAULT_BUCKETS` tạo bucket). `minio-init` chờ MinIO thật sự nhận kết nối (Bitnami khởi động 2 lần).
- **Airflow image**: providers cài theo đúng constraints 2.9.3 (hết `ResolutionImpossible`); thêm `psycopg`, `pytest`; `pyarrow<19` (pyarrow mới đòi numpy ≥ 2, constraints giữ numpy 1.26); `PYTHONPATH=/opt/ingestion/src` để `coffee_ingestion` import được từ bind mount.
- **Jupyter image**: bỏ `s3fs` (kéo `fsspec` đã bị yank → pip backtracking vô tận); không lab nào dùng.
- **Iceberg catalog**: đã kiểm Trino 460 có sẵn driver postgres trong plugin iceberg; Spark và Trino đọc cùng bảng qua JDBC catalog.
- Port Marquez API trên host mặc định **5050** (macOS dùng 5000 cho AirPlay Receiver). Trong network vẫn `marquez-api:5000`.

### Fixed — code lộ ra khi chạy thật
- `coffee_daily_pipeline`: `{{ ds }}` là ngày **UTC** → với lịch 02:00 giờ VN lệch đúng 1 ngày. Task `business_date` giờ đổi `data_interval_start` về Asia/Ho_Chi_Minh; `coffee_quality_checks` lấy ngày từ XCom của run upstream (hoặc `params.date` khi trigger tay). Task `notify` gửi Slack/Telegram qua Airflow Variable (`SLACK_WEBHOOK_URL`, `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`), lỗi gửi không làm pipeline đỏ.
- `dbt_build` trong Airflow dùng `--target-path target_airflow`: dbt 1.11 (image) và 1.9 (máy dev) ghi layout `target/` khác nhau, dùng chung qua bind mount sẽ lỗi `Not a directory` ở snapshot.
- `coffee_ingestion.Lake.write_parquet`: overwrite **cả partition** (xoá file khác tên còn lại, ví dụ `part-00000-<uuid>.parquet` + `_SUCCESS` của Spark) — trước đó hai writer luân phiên để lại hai bản dữ liệu → dbt `unique` fail. Xoá từng object vì MinIO từ chối bulk delete thiếu `Content-MD5`.
- `coffee_ingestion.read_postgres`: `SET TIME ZONE 'Asia/Ho_Chi_Minh'` — session UTC làm hoá đơn 5–7h sáng rơi vào partition hôm trước (35 duplicate xuyên partition khi backfill từ Postgres rồi ingest từ CSV).
- `contract_check.py` / `run_checkpoint.py`: đọc `s3://` qua MinIO cần `storage_options` cho Polars (`aws_endpoint_url`, `aws_allow_http`) — trước đó âm thầm gọi AWS thật và `InvalidAccessKeyId`.
- `videos/13 consumer.py`: Ctrl+C đúng lúc vừa commit → `KafkaException _NO_OFFSET` làm chương trình crash; giờ bỏ qua mã đó.
- `videos/18 latency_probe.py`: `window` là từ khoá DuckDB → `"window".start`.
- `platform/airflow/dags/test_dag_integrity.py`: `DAG.test_cycle()` không tồn tại ở Airflow 2.9 → `check_cycle()`. Thêm `.airflowignore` để scheduler không parse file test.
- `videos/23`: policy MinIO phải tách `s3:GetBucketLocation` khỏi condition `s3:prefix` (mc 2024-10 từ chối).

### Added
- `platform/observability/dbt_lineage.py`: gửi lineage model-level của dbt lên Marquez từ `manifest.json` + `run_results.json` (stdlib). Lý do: `dbt-ol` (openlineage-dbt 1.18) không hỗ trợ adapter duckdb.
- Output thật dán vào "Kết quả mong đợi" của #02, #03, #05, #08, #09, #10, #11 (bảng Summary Metrics 4 mode từ event log), #13, #14, #15 (bảng watermark/tz_drift: ~63 % hoá đơn bị bỏ), #16, #17, #18 (p50/p95), #20, #21, #23 (giá AWS từ Price List API 22/10/2026). Vài kết luận trong README đã **đổi** vì số thật khác dự đoán: plan OLAP Postgres là `GroupAggregate` không phải `HashAggregate` (#02); Spark UI 14 job và 1 task/stage do AQE (#10); AQE gộp 8 partition thành 1 task thay vì tách skew ở 5 M dòng (#11); `.snapshots` có 3 dòng khi dùng `iceberg_silver.py` (#17).
- SETUP.md: bảng troubleshooting thêm 4 tình huống gặp thật (image bị gỡ, port 5000/8081, Airflow image cũ, topic chưa tồn tại với Structured Streaming); RAM đo thật theo profile.

### Notes
- Số đo thời gian (ms, s) trong README là của máy mình (Apple M-series, Docker Desktop 7,75 GiB); hình dạng kết quả mới là thứ cần giống.
- Chưa thử trên Linux x86_64 và Windows/WSL2 — image đều multi-arch nhưng chưa có người chạy.

## [0.1.0] — 2026-10-22 — Scaffold ban đầu

### Added
- **Repo skeleton**: MIT (code) + CC BY-NC-SA 4.0 (nội dung), Makefile (`up/down/reset/seed/logs/test/lint/dbt-parse/smoke/generate`), `pyproject.toml` workspace uv (ruff + pytest), pre-commit, `.env.example`.
- **Môi trường Docker** (`docker-compose.yml`, video #05): postgres 16 (seed từ `data/samples`, `wal_level=logical`, publication Debezium), MinIO + bucket `lake`/`warehouse`, Spark 3.5 (Iceberg + hadoop-aws + Kafka jars), Kafka 3.8 KRaft + Schema Registry + Kafka UI, Debezium 2.7, Airflow 2.9 LocalExecutor (+ providers, dbt, GX), Marquez 0.49, Trino 460, Jupyter PySpark. Profiles `core | notebook | streaming | spark | orchestration | analytics`; `make up` = core+notebook cho máy 8 GB. `SETUP.md` hướng dẫn Win/Mac/Linux, RAM theo profile, troubleshooting.
- **Generator `coffeegen`** (`data/generator`, stdlib-only, deterministic): 30 cửa hàng theo quận HN/HCMC, menu VND, giờ cao điểm, Tết 2024–2028, mưa theo mùa, ~1.5% lỗi cố ý (duplicate, null store_id, tz drift, negative qty, late event, null reading). CLI `pos | app-events | iot | stream` (Kafka producer, `--dry-run`). `SCHEMA.md` tiếng Việt. 16 test, kiểm tra byte-level determinism.
- **Samples** (`data/samples`, 1.9 MB): 14→19/02/2026 bao trùm Tết; SQL init cho Postgres; CI kiểm tra `make generate` không đổi gì.
- **Platform**:
  - `coffee-ingest` (`platform/ingestion`, polars): source → bronze (raw) → silver (typed, dedup, tz fix, quarantine), local hay `s3://`; 8 test.
  - dbt `coffee_gold` (`platform/dbt`): staging + star schema (`dim_store`, `dim_menu_item`, `dim_customer`, `dim_date`, `fct_sales`, `fct_daily_store_revenue`), generic test tự viết, snapshot SCD2, mask PII; target `dev` = DuckDB đọc parquet, target `ci` cho `dbt parse`. `dbt build` 45/45 PASS trên samples.
  - Spark jobs (`platform/spark`): bronze→silver, skew demo 4 mode, Structured Streaming Kafka→cửa sổ 1 phút, Iceberg MERGE/time travel. Iceberg catalog `lake` = JDBC catalog trong Postgres (db `iceberg_catalog`) dùng chung Spark ↔ Trino.
  - Airflow DAGs: `coffee_daily_pipeline`, `coffee_backfill`, `coffee_quality_checks` (Dataset-triggered) + DAG integrity test.
  - Quality: data contract YAML + `contract_check.py`, suite Great Expectations 1.x, `run_checkpoint.py`.
  - Observability: Marquez config, OpenLineage cho Spark/dbt, `emit_lineage.py`.
  - Lake conventions + Iceberg DDL (`platform/lake`).
- **Terraform tuỳ chọn** (`infra/terraform`): S3 bucket (versioning, SSE, lifecycle), Glue database, IAM least privilege. `fmt`/`validate` chạy trong CI.
- **25 thư mục video** (`videos/00-intro` → `24-portfolio-phong-van`): README tiếng Việt (mục tiêu, lab từng bước, Kết quả mong đợi với số thật từ samples, câu hỏi phỏng vấn, bài tập), `lab/` starter có TODO + `lab/solution/`, `slides/` placeholder. 76 test offline cho lab (tổng repo: 100 test, 1 skip).
- **Docs**: README (kiến trúc Mermaid, lịch phát hành Tue/Fri 19:00 từ 03/11/2026), `docs/architecture.md` (sơ đồ theo từng phần + quyết định), `docs/glossary.md`, `docs/faq.md`.
- **CI** (GitHub Actions): ruff lint/format, pytest, `dbt parse`, smoke (generate → diff → coffee-ingest → contract check → dbt build), `docker compose config`, `terraform fmt/validate`. Không cần service nào.
- Issue templates (bug, lab question), PR template.

### Notes
- YouTube link toàn bộ là `TODO:youtube-link`; trạng thái video đều ⚪ cho tới khi phát hành.
- Polars trong lockfile là 2.x: tránh `cast(pl.Date)` từ string và `is_in(Series)`.

---

## TODO còn lại cho Thuỷ (`grep -rn "TODO(thuy)"` → chỉ còn mục nội dung)

### Nội dung — chỉ làm được khi quay/phát hành
- Đổi `TODO:youtube-link` và ⚪ → 🟢 trong README theo lịch phát hành.
- #19: `sla` cho DAG hiện để làm bài tập; webhook Slack/Telegram đã nối qua Variable, cần điền giá trị thật khi demo.

### Nên làm trước khi phát hành phần 3–4
- Chạy một lượt `make up PROFILE=all` trên **Linux x86_64** và **Windows/WSL2** (mình chỉ chạy trên macOS arm64) — quan tâm nhất: image `bitnamilegacy/minio` và `apache/spark` kéo được, Airflow build ~5 phút.
- `bitnamilegacy/*` là kho lưu trữ không cập nhật; nếu sau này bị gỡ nốt, phương án là tự build MinIO từ binary (`dl.min.io`) — để sẵn một `minio.Dockerfile` khi cần.
- dbt 1.9 trên máy dev đang bị đánh dấu deprecated (warning vô hại); image Airflow đã dùng dbt 1.11. Nâng lock khi dbt-duckdb hỗ trợ.
- Trên máy có process khác chiếm `127.0.0.1:9000` (mình gặp), DuckDB/Polars đôi lúc nói chuyện nhầm đích; đổi `MINIO_PORT` trong `.env` nếu thấy lỗi `invalid HTTP version`.
