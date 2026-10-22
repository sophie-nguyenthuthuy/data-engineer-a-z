-- Lab #16: tạo thay đổi trong Postgres để Debezium bắt. Chạy TỪNG khối, nhìn Kafka UI / console consumer giữa các khối.
-- docker compose exec -T postgres psql -U coffee -d coffee_pos < videos/16-cdc-debezium/lab/solution/cdc_demo.sql

-- 0. Bảng raw không có PRIMARY KEY (cố ý, xem 01_schema.sql). pgoutput từ chối UPDATE/DELETE trên bảng
--    được publish mà không có replica identity → bật FULL để Debezium nhận cả `before`.
ALTER TABLE pos_transactions REPLICA IDENTITY FULL;
ALTER TABLE pos_line_items   REPLICA IDENTITY FULL;

-- 1. INSERT → envelope op = "c", before = null
INSERT INTO pos_transactions
    (transaction_id, store_id, ts, channel, payment_method, customer_id, staff_id, total_amount_vnd, discount_vnd, is_rainy_day)
VALUES
    ('HN01-20260215-99001', 'HN01', '2026-02-15 10:00:00+07', 'store', 'cash', NULL, 'HN01-S1', 45000, 0, false);

-- 2. UPDATE → op = "u", before.total_amount_vnd = 45000, after.total_amount_vnd = 50000
UPDATE pos_transactions SET total_amount_vnd = 50000 WHERE transaction_id = 'HN01-20260215-99001';

-- 3. DELETE → op = "d", after = null, rồi một message tombstone (value = null) cùng key
DELETE FROM pos_transactions WHERE transaction_id = 'HN01-20260215-99001';

-- 4. Kiểm tra slot & lag WAL (nếu connector chết, cột này lớn dần cho tới khi đầy đĩa)
SELECT slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained_wal
FROM pg_replication_slots;
