-- Lab #23 — role Postgres chỉ-đọc cho analyst, KHÔNG thấy cột PII của customers.
-- Chạy:  docker compose exec -T postgres psql -U coffee -d coffee_pos < videos/23-chi-phi-bao-mat/lab/analyst_role.sql
-- Kiểm tra ở cuối file. Hoàn tác: DROP OWNED BY analyst; DROP ROLE analyst;

-- 1) Role đăng nhập được, mật khẩu chỉ dùng cho lab
DROP ROLE IF EXISTS analyst;
CREATE ROLE analyst LOGIN PASSWORD 'analyst';

-- 2) Quyền tối thiểu để vào được database và schema
GRANT CONNECT ON DATABASE coffee_pos TO analyst;
GRANT USAGE ON SCHEMA public TO analyst;

-- 3) TODO: GRANT SELECT trên 4 bảng không có PII: stores, menu_items, pos_transactions, pos_line_items
--    (KHÔNG dùng ALL TABLES — sẽ cấp luôn customers)


-- 4) TODO: customers — cấp SELECT theo CỘT, bỏ full_name / phone / email:
--    GRANT SELECT (customer_id, city, joined_date, tier) ON customers TO analyst;


-- 5) TODO: view customers_masked cho trường hợp analyst cần đối soát 3 số cuối:
--    CREATE OR REPLACE VIEW customers_masked AS SELECT customer_id, city, joined_date, tier,
--      repeat('*', greatest(length(phone) - 3, 0)) || right(phone, 3) AS phone_masked FROM customers;
--    rồi GRANT SELECT ON customers_masked TO analyst;
--    (so sánh với macro mask_pii ở platform/dbt/macros/mask_pii.sql — cùng một quy tắc, hai nơi)


-- 6) Bảng tạo SAU này cũng không lọt: default privileges chỉ cấp SELECT cho bảng mới của owner coffee
ALTER DEFAULT PRIVILEGES FOR ROLE coffee IN SCHEMA public GRANT SELECT ON TABLES TO analyst;
-- Lưu ý: dòng trên cấp SELECT mọi cột cho bảng mới. Bảng mới có PII thì phải REVOKE tay — ghi vào checklist review.

-- ── Kiểm tra (chạy trong cùng session, không cần mật khẩu) ─────────────────────
SET ROLE analyst;
SELECT count(*) AS n_txn FROM pos_transactions;                       -- OK
SELECT customer_id, city, tier FROM customers LIMIT 2;                -- OK (cột được phép)
SELECT phone FROM customers LIMIT 1;                                  -- ERROR: permission denied for table customers
RESET ROLE;
