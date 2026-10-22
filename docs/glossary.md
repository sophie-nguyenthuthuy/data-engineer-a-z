# Glossary — thuật ngữ dùng trong khoá

Mình giữ thuật ngữ tiếng Anh khi nói và viết (vì JD, docs, StackOverflow đều tiếng Anh), cột tiếng Việt chỉ để bạn hiểu nghĩa.

| Thuật ngữ | Tiếng Việt / giải thích ngắn | Video |
|-----------|------------------------------|-------|
| **OLTP** | Online Transaction Processing — DB phục vụ giao dịch (POS ghi hoá đơn). Row store, nhiều write nhỏ. | #02 |
| **OLAP** | Online Analytical Processing — DB phục vụ phân tích. Column store, ít query nhưng quét nhiều dòng. | #02 |
| **Data Warehouse** | Kho dữ liệu đã mô hình hoá (star schema), schema-on-write. | #02, #06 |
| **Data Lake** | Lưu file thô (Parquet/JSON) trên object storage, schema-on-read. | #02, #09 |
| **Lakehouse** | Lake + table format (Iceberg/Delta/Hudi) để có ACID, time travel, schema evolution. | #17 |
| **ETL / ELT** | Extract-Transform-Load vs Extract-Load-Transform. Khác nhau ở *chỗ* biến đổi: trước khi load (ETL) hay trong warehouse (ELT). | #07 |
| **Bronze / Silver / Gold** (medallion) | Raw → sạch, typed → sẵn cho BI. | #09 |
| **Partition** | Chia dữ liệu theo giá trị cột (vd `dt=2026-02-15`) để query chỉ đọc phần cần. | #09, #11 |
| **Predicate pushdown** | Engine đẩy điều kiện lọc xuống tầng đọc file, bỏ qua row group không khớp (nhờ min/max statistics). | #08 |
| **Row group** | Khối dòng trong file Parquet, có statistics riêng. | #08 |
| **Schema evolution** | Thêm/đổi cột mà không phải ghi lại toàn bộ dữ liệu. | #08, #17 |
| **Window function** | Hàm tính trên "cửa sổ" dòng liên quan (`ROW_NUMBER() OVER (PARTITION BY …)`), không gộp dòng như GROUP BY. | #03 |
| **CTE** | Common Table Expression — `WITH x AS (...)`, đặt tên cho subquery. | #03 |
| **Query plan** | Cách DB sẽ thực thi query; đọc bằng `EXPLAIN (ANALYZE)`. | #03 |
| **Star schema** | 1 bảng fact ở giữa, các bảng dimension xung quanh. | #06 |
| **Fact / Dimension** | Fact = sự kiện đo được (hoá đơn, line item). Dimension = ngữ cảnh (cửa hàng, món, ngày). | #06 |
| **Grain** | Một dòng fact đại diện cho cái gì (fct_sales: 1 line item). | #06 |
| **SCD Type 2** | Slowly Changing Dimension — giữ lịch sử thay đổi bằng cột valid_from/valid_to. | #06, #12 |
| **Data Vault** | Mô hình hub/link/satellite, tối ưu cho tải và audit, không cho query trực tiếp. | #06 |
| **Idempotent** | Chạy lại nhiều lần cho cùng kết quả (ghi đè partition thay vì append). | #07, #19 |
| **Quarantine** | Bảng/thư mục chứa dòng lỗi thay vì xoá. | #07, #20 |
| **Lazy evaluation** | Xây kế hoạch trước, chỉ chạy khi cần kết quả (Polars lazy, Spark). | #04, #10 |
| **Apache Arrow** | Định dạng cột in-memory chung, copy không mất phí giữa pandas/Polars/DuckDB/Spark. | #04 |
| **Driver / Executor** | Spark: driver lập kế hoạch, executor chạy task. | #10 |
| **Shuffle** | Spark chuyển dữ liệu giữa executor (join, group by) — bước đắt nhất. | #10, #11 |
| **Data skew** | Một key chiếm phần lớn dữ liệu → một task làm hết việc. | #11 |
| **Salting** | Thêm số ngẫu nhiên vào key để chia nhỏ key skew. | #11 |
| **Broadcast join** | Gửi bảng nhỏ tới mọi executor, tránh shuffle bảng lớn. | #11 |
| **AQE** | Adaptive Query Execution — Spark tự điều chỉnh plan lúc chạy (gộp partition, xử lý skew). | #11 |
| **Small files problem** | Quá nhiều file nhỏ → overhead mở file lớn hơn đọc dữ liệu. | #11 |
| **dbt** | Data build tool — viết T bằng SQL + Jinja, có test/docs/lineage. `ref()`, `source()`. | #12 |
| **Materialization** | dbt: view / table / incremental / snapshot. | #12 |
| **Topic / Partition / Offset** | Kafka: luồng message / chia nhỏ để song song / vị trí trong partition. | #13 |
| **Consumer group** | Nhóm consumer chia nhau partition của topic. | #13 |
| **KRaft** | Kafka chạy không cần ZooKeeper. | #13 |
| **At-most / at-least / exactly-once** | Đảm bảo giao message: có thể mất / có thể trùng / đúng một lần. | #14 |
| **DLQ** | Dead Letter Queue — topic chứa message xử lý thất bại. | #14 |
| **Schema Registry** | Dịch vụ lưu schema (Avro/Protobuf/JSON) và kiểm tra tương thích. | #14 |
| **Event time / Processing time** | Lúc sự kiện xảy ra vs lúc hệ thống nhận. | #15 |
| **Watermark** | Ngưỡng "muộn tới đâu còn chấp nhận" trong streaming. | #15 |
| **Window (streaming)** | Cửa sổ thời gian để gộp (tumbling 1 phút, sliding, session). | #15 |
| **CDC** | Change Data Capture — bắt INSERT/UPDATE/DELETE từ WAL của DB. | #16 |
| **WAL / logical replication** | Write-Ahead Log của Postgres; `wal_level=logical` cho phép Debezium đọc. | #16 |
| **Iceberg / Delta / Hudi** | Ba table format mở cho lakehouse. | #17 |
| **Time travel** | Query dữ liệu ở snapshot cũ. | #17 |
| **MERGE INTO** | Upsert: match thì update, không thì insert. | #17 |
| **Lambda / Kappa** | Kiến trúc batch + streaming song song / chỉ streaming. | #18 |
| **DAG** | Directed Acyclic Graph — đồ thị task trong Airflow. | #19 |
| **Backfill** | Chạy lại pipeline cho khoảng ngày quá khứ. | #19 |
| **Catchup** | Airflow tự chạy các lần bị bỏ lỡ từ start_date. | #19 |
| **Data contract** | Hợp đồng schema + ràng buộc giữa team nguồn và team data. | #20 |
| **Great Expectations** | Thư viện kiểm tra chất lượng dữ liệu bằng "expectation". | #20 |
| **Freshness** | Dữ liệu mới nhất cách hiện tại bao lâu. | #20, #21 |
| **Lineage** | Dòng chảy dữ liệu: bảng nào sinh ra từ bảng nào bởi job nào. | #21 |
| **OpenLineage / Marquez** | Chuẩn mở cho lineage event / server lưu và vẽ lineage. | #21 |
| **CI/CD** | Tự động lint/test khi push; tự động deploy khi merge. | #22 |
| **IaC / Terraform** | Infrastructure as Code — hạ tầng khai báo bằng file, có plan/apply/destroy. | #22 |
| **FinOps** | Quản lý chi phí cloud như một kỷ luật kỹ thuật. | #23 |
| **PII** | Personally Identifiable Information — tên, SĐT, email… | #23 |
| **Least privilege** | Chỉ cấp quyền tối thiểu cần để làm việc. | #23 |
| **Masking / Tokenization** | Che một phần giá trị / thay bằng token không đảo ngược được. | #23 |
