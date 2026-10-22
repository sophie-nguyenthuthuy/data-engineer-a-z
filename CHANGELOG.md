# Changelog

Định dạng theo [Keep a Changelog](https://keepachangelog.com/vi/1.0.0/), commit theo Conventional Commits.

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

## TODO còn lại cho Thuỷ (`grep -rn "TODO(thuy)"`)

### Cần chạy Docker/cluster để lấy output thật (dán vào "Kết quả mong đợi" sau khi quay)
| Video | Việc |
|-------|------|
| #02, #03 | output `EXPLAIN (ANALYZE, BUFFERS)` thật của Postgres |
| #05 | `make ps` + `docker stats` thật |
| #08 | output Avro schema evolution trong Jupyter |
| #09 | `mc ls/du` thật trên MinIO |
| #10 | log `spark-submit` thật + ảnh Spark UI (Jobs/Stages/SQL DAG) |
| #11 | bảng Summary Metrics 4 mode; kiểm tra AQE có tách skew partition với 5 M dòng (ngưỡng 256 MB) |
| #13 | `--describe` consumer group + vài dòng consumer |
| #14 | output `dlq_consumer` + response 409 của Schema Registry |
| #15 | console Batch thật; số cửa sổ mất khi watermark 1 phút với tz_drift |
| #16 | status JSON connector + 4 envelope c/u/d thật |
| #17 | `.snapshots` + `metadata_explorer` trên `./tmp-meta`; **kiểm chứng Trino 460 đọc bảng Spark tạo qua JDBC catalog** (driver postgres trong plugin iceberg — nếu thiếu, mount jar) |
| #18 | `make ps` toàn bộ + p50/p95 latency (README và runbook) |
| #19 | ảnh Graph + `ds` thật; đặt `sla` cho DAG (hiện là bài tập) |
| #20 | output GX thật (xác nhận 8/8 trên samples); số `dbt test` PASS |
| #21 | ảnh graph Marquez; cách provider openlineage 1.9.1 đặt tên job/dataset; output `dbt-ol` |
| #22 | cập nhật bảng job khi `ci.yml` đổi; số resource từ `terraform plan` (hiện ghi 12) |
| #23 | xác nhận giá S3/Athena ap-southeast-1 (README + 2 file `cost_model.py`); cú pháp `mc admin policy create` với image MinIO 2024-10 |

### Platform
- `platform/spark/*.py`: đã lint, **chưa chạy end-to-end trên cluster compose** — chạy thử 4 job trước khi quay #10/#11/#15/#17.
- `platform/airflow/dags/coffee_daily_pipeline.py`: nối Slack/Telegram webhook ở task `notify` (#19).
- `infra/docker/trino/catalog/lake.properties`: xác nhận driver `org.postgresql.Driver` có trong plugin iceberg của Trino 460.
- Build 3 image tuỳ biến lần đầu (`make up PROFILE=all`) trên cả Mac M-series và Linux x86 để chắc tất cả jar tải được và `healthcheck` lên.
- dbt 1.9 đang bị đánh dấu deprecated khi chạy (warning vô hại); nâng khi dbt-duckdb hỗ trợ bản mới hơn.

### Nội dung
- Thêm PDF slide vào `videos/NN-*/slides/` sau mỗi video; đổi `TODO:youtube-link` và ⚪ → 🟢 trong README.
- Bật GitHub Discussions cho repo (README và issue config đã trỏ tới).
- Cân nhắc thêm GitHub Actions badge thật sau lần CI xanh đầu tiên (badge đã trỏ đúng workflow `ci.yml`).
