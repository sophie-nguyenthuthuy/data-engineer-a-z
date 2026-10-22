# Lab #01 — Lộ trình 24 video ↔ kỹ năng trong JD Data Engineer

Cột **Kỹ năng JD** là những cụm từ hay gặp trong JD Data Engineer ở Việt Nam (mình tổng hợp từ các JD junior/mid: SQL, Python, Spark, Kafka, Airflow, cloud (AWS/GCP), dbt, data modeling, Docker, CI/CD, data quality).
Cột **JD của bạn**: bạn tự điền — mở 3 JD thật, mỗi yêu cầu trong JD tìm video tương ứng và đánh `✓` (hoặc ghi tên JD).

| # | Video | Kỹ năng JD | Mức sau khi học | JD của bạn |
|---|-------|------------|-----------------|------------|
| 00 | Giới thiệu & cách dùng repo | Git, đọc README | — | |
| 01 | Data Engineer là gì | hiểu vai trò, vẽ data flow | — | TODO |
| 02 | Kiến trúc dữ liệu: OLTP/OLAP/Warehouse/Lake | "data warehouse / data lake / lakehouse" | hiểu, giải thích được | TODO |
| 03 | SQL cho DE | **SQL** (window function, CTE, query plan) | làm được bài test SQL | TODO |
| 04 | Python cho DE | **Python**, pandas / Polars / DuckDB | viết pipeline nhỏ | TODO |
| 05 | Docker cho DE | **Docker**, docker compose | dựng môi trường local | TODO |
| 06 | Data Modeling | **data modeling**: star schema, SCD2, Data Vault | thiết kế mart | TODO |
| 07 | ETL vs ELT | **ETL/ELT**, bronze/silver/gold, idempotent | viết batch pipeline | TODO |
| 08 | Định dạng file | Parquet / Avro / ORC, columnar | chọn format đúng | TODO |
| 09 | Data Lake trên S3 | **S3 / MinIO**, partitioning, lifecycle | tổ chức lake | TODO |
| 10 | Apache Spark | **Spark** (DataFrame API, cluster) | viết job Spark | TODO |
| 11 | Tối ưu Spark | shuffle, skew, partition, broadcast join | debug job chậm | TODO |
| 12 | dbt | **dbt** (models, tests, snapshots, docs) | dựng gold layer | TODO |
| 13 | Kafka | **Kafka** (topic, partition, consumer group) | produce/consume | TODO |
| 14 | Delivery semantics, DLQ, Schema Registry | exactly-once, Avro schema evolution | thiết kế topic an toàn | TODO |
| 15 | Spark Structured Streaming | **streaming**, watermark, late data | job streaming | TODO |
| 16 | CDC với Debezium | **CDC**, Debezium, WAL | sync DB → Kafka | TODO |
| 17 | Lakehouse: Iceberg / Delta / Hudi | **lakehouse**, table format, time travel | chọn & dùng Iceberg | TODO |
| 18 | Kiến trúc streaming end-to-end | Lambda vs Kappa, tradeoff | thiết kế hệ thống | TODO |
| 19 | Airflow | **Airflow** (DAG, sensor, backfill) | lịch pipeline | TODO |
| 20 | Data Quality | **Great Expectations**, data contract | gate chất lượng | TODO |
| 21 | Observability & Lineage | OpenLineage, Marquez, SLA | debug nhanh | TODO |
| 22 | CI/CD & Terraform | **CI/CD**, GitHub Actions, **Terraform**, **AWS** | deploy có kiểm soát | TODO |
| 23 | Chi phí & bảo mật | cost, IAM, PII masking | nói chuyện với sếp | TODO |
| 24 | Portfolio & phỏng vấn | trình bày dự án, câu hỏi phỏng vấn | đi phỏng vấn | TODO |

## Kỹ năng hay gặp trong JD mà khoá KHÔNG đi sâu

Khoá chọn một stack (Postgres · MinIO · Spark · Kafka · dbt · Airflow · Iceberg · Trino) để làm **tới cùng** thay vì lướt 20 công cụ. Những thứ dưới đây bạn sẽ gặp trong JD, và kiến thức khoá dạy **chuyển sang được** (cột phải):

| Kỹ năng trong JD | Khoá dạy cái tương đương | Tự học thêm |
|------------------|--------------------------|-------------|
| Snowflake / BigQuery / Redshift | DuckDB + Trino (cùng mô hình OLAP column store, #02, #17) | docs của vendor, 1 ngày |
| Databricks / Delta Lake | Spark (#10, #11) + Iceberg (#17); Delta có so sánh ở #17 | Databricks Community Edition |
| Scala | Spark bằng Python (PySpark) — API giống nhau | Scala cơ bản 1 tuần |
| Flink | Spark Structured Streaming (#15), cùng khái niệm watermark/state | Flink SQL docs |
| Azure Data Factory / AWS Glue | Airflow (#19) — cùng khái niệm DAG, schedule, retry | 1 tutorial của vendor |
| Kubernetes | Docker Compose (#05) — bước trung gian hợp lý | KCD / kind local |
| Power BI / Tableau / Looker | Khoá chỉ dừng ở gold layer (#12); dashboard là việc của DA | Metabase OSS 1 buổi |

## Phần của bạn

Dán 3 JD (ẩn tên công ty nếu muốn), mỗi JD liệt kê yêu cầu → số video:

### JD 1: TODO (vị trí, mức junior/mid/senior)
- TODO yêu cầu → #TODO
- …

### JD 2: TODO
- …

### JD 3: TODO
- …

### Thiếu (yêu cầu không có video nào dạy)
- TODO
