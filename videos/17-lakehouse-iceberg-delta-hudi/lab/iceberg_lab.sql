-- Lab #17 — Iceberg: MERGE INTO idempotent, snapshots, time travel, schema evolution.
-- Chạy từng khối trong spark-sql shell:
--   docker compose exec spark-master spark-sql --master spark://spark-master:7077
-- Catalog `lake` (Iceberg, hadoop, warehouse s3a://warehouse/) cấu hình ở infra/docker/spark-defaults.conf.
-- DDL mẫu: platform/lake/iceberg/silver_tables.sql. Job Python tương ứng: platform/spark/iceberg_silver.py.

-- 1. Namespace + bảng.
CREATE NAMESPACE IF NOT EXISTS lake.silver;

-- TODO 1: CREATE TABLE IF NOT EXISTS lake.silver.pos_transactions (...) USING iceberg
--   Chép đúng 11 cột từ platform/lake/iceberg/silver_tables.sql, PARTITIONED BY (days(ts)),
--   TBLPROPERTIES ('format-version' = '2'). Vì sao partition theo days(ts) mà không cần cột dt?


-- 2. Nguồn: silver Parquet ngày chuẩn.
CREATE OR REPLACE TEMPORARY VIEW incoming AS
SELECT * FROM parquet.`s3a://lake/silver/pos_transactions/dt=2026-02-15`;

SELECT count(*) AS incoming_rows FROM incoming;

-- 3. TODO 2: MERGE INTO lake.silver.pos_transactions t USING incoming s ON <khoá>
--   WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *;
--   rồi SELECT count(*). Chạy khối này HAI LẦN và so count.


-- 4. TODO 3: xem bảng metadata `.snapshots`: snapshot_id, committed_at, operation,
--   summary['added-records'], summary['deleted-records']. Operation của lần MERGE thứ 2 là gì? Vì sao?


-- 5. TODO 4: time travel — SELECT count(*) ... VERSION AS OF <snapshot_id đầu tiên>.
--   Thử thêm TIMESTAMP AS OF '<thời điểm giữa 2 lần merge>'.


-- 6. TODO 5: schema evolution — ALTER TABLE ... ADD COLUMN source_system STRING; SELECT 3 dòng.
--   Có file Parquet nào được ghi lại không? Kiểm tra bằng `.files` trước và sau.


-- 7. Xem thêm: lake.silver.pos_transactions.files / .partitions / .history
