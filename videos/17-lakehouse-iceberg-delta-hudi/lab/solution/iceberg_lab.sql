-- Lời giải lab #17 — Iceberg: MERGE INTO idempotent, snapshots, time travel, schema evolution.
-- Chạy từng khối trong spark-sql shell (khối 6 cần snapshot_id lấy từ khối 5):
--   docker compose exec spark-master spark-sql --master spark://spark-master:7077
-- hoặc cả file (trừ khối 6):
--   docker compose cp videos/17-lakehouse-iceberg-delta-hudi/lab/solution/iceberg_lab.sql spark-master:/tmp/
--   docker compose exec spark-master spark-sql --master spark://spark-master:7077 -f /tmp/iceberg_lab.sql
-- Catalog `lake` (Iceberg, kiểu hadoop, warehouse s3a://warehouse/) cấu hình ở infra/docker/spark-defaults.conf.

-- 1. Namespace + bảng. DDL giống platform/lake/iceberg/silver_tables.sql (chỉ lấy bảng pos_transactions).
CREATE NAMESPACE IF NOT EXISTS lake.silver;

CREATE TABLE IF NOT EXISTS lake.silver.pos_transactions (
    transaction_id   STRING    NOT NULL,
    store_id         STRING    NOT NULL,
    ts               TIMESTAMP NOT NULL,
    channel          STRING,
    payment_method   STRING,
    customer_id      STRING,
    staff_id         STRING,
    total_amount_vnd BIGINT,
    discount_vnd     BIGINT,
    is_rainy_day     BOOLEAN,
    _tz_fixed        BOOLEAN
)
USING iceberg
PARTITIONED BY (days(ts))                      -- hidden partitioning: người query không cần biết cột dt
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');

-- 2. Nguồn: silver Parquet ngày chuẩn (ghi bởi coffee-ingest / bronze_to_silver_pos ở #09–#10).
CREATE OR REPLACE TEMPORARY VIEW incoming AS
SELECT * FROM parquet.`s3a://lake/silver/pos_transactions/dt=2026-02-15`;

SELECT count(*) AS incoming_rows FROM incoming;          -- 792

-- 3. MERGE lần 1: bảng rỗng → toàn bộ là INSERT.
MERGE INTO lake.silver.pos_transactions t
USING incoming s
ON t.transaction_id = s.transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

SELECT count(*) AS after_merge_1 FROM lake.silver.pos_transactions;   -- 792

-- 4. MERGE lần 2, y hệt: mọi dòng MATCHED → UPDATE, không có INSERT → KHÔNG duplicate.
--    (So với #09: ghi đè partition bằng tay. Ở đây engine lo, và còn giữ lịch sử.)
MERGE INTO lake.silver.pos_transactions t
USING incoming s
ON t.transaction_id = s.transaction_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

SELECT count(*) AS after_merge_2 FROM lake.silver.pos_transactions;   -- vẫn 792

-- 5. Snapshots: mỗi commit một dòng. MERGE lần 2 là `overwrite` (copy-on-write: xoá 792, thêm 792).
SELECT snapshot_id, committed_at, operation,
       summary['added-records']   AS added,
       summary['deleted-records'] AS deleted,
       summary['total-records']   AS total
FROM lake.silver.pos_transactions.snapshots
ORDER BY committed_at;

-- 6. Time travel — thay <SNAPSHOT_ID> bằng id của snapshot ĐẦU từ khối 5.
-- SELECT count(*) FROM lake.silver.pos_transactions VERSION AS OF <SNAPSHOT_ID>;
-- SELECT count(*) FROM lake.silver.pos_transactions TIMESTAMP AS OF '2026-10-22 10:00:00';
-- Không rollback thật trong lab, nhưng đây là lệnh: CALL lake.system.rollback_to_snapshot('silver.pos_transactions', <SNAPSHOT_ID>);

-- 7. Schema evolution: thêm cột là thao tác metadata — không đọc lại, không ghi lại file dữ liệu nào.
ALTER TABLE lake.silver.pos_transactions ADD COLUMN source_system STRING COMMENT 'pos | app | cdc';
SELECT transaction_id, total_amount_vnd, source_system FROM lake.silver.pos_transactions LIMIT 3;  -- cột mới NULL

-- Cột được định danh bằng id, không bằng tên → đổi tên an toàn, file Parquet cũ vẫn đọc đúng:
ALTER TABLE lake.silver.pos_transactions RENAME COLUMN source_system TO _source;

-- 8. Bảng metadata khác đáng xem.
SELECT file_path, record_count, file_size_in_bytes FROM lake.silver.pos_transactions.files;
SELECT partition, record_count, file_count FROM lake.silver.pos_transactions.partitions;
SELECT made_current_at, snapshot_id, is_current_ancestor FROM lake.silver.pos_transactions.history;

-- 9. Dọn snapshot cũ (production chạy định kỳ; ở lab chỉ xem cú pháp — chạy thì mất time travel):
-- CALL lake.system.expire_snapshots(table => 'silver.pos_transactions', older_than => TIMESTAMP '2026-12-31 00:00:00', retain_last => 1);
