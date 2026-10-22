-- Lời giải lab #23 — role Postgres chỉ-đọc cho analyst, không thấy cột PII của customers.
-- Chạy:  docker compose exec -T postgres psql -U coffee -d coffee_pos < videos/23-chi-phi-bao-mat/lab/solution/analyst_role.sql
-- Hoàn tác: DROP OWNED BY analyst; DROP ROLE analyst;

DROP ROLE IF EXISTS analyst;
CREATE ROLE analyst LOGIN PASSWORD 'analyst';

GRANT CONNECT ON DATABASE coffee_pos TO analyst;
GRANT USAGE ON SCHEMA public TO analyst;

-- Bảng không có PII: cấp cả bảng
GRANT SELECT ON stores, menu_items, pos_transactions, pos_line_items TO analyst;

-- customers: column-level GRANT — Postgres hỗ trợ từ 8.4, nhưng ít người biết
GRANT SELECT (customer_id, city, joined_date, tier) ON customers TO analyst;

-- View masked cho nhu cầu đối soát: cùng quy tắc với platform/dbt/macros/mask_pii.sql (giữ 3 số cuối)
CREATE OR REPLACE VIEW customers_masked AS
SELECT
    customer_id,
    city,
    joined_date,
    tier,
    repeat('*', greatest(length(phone) - 3, 0)) || right(phone, 3) AS phone_masked
FROM customers;
GRANT SELECT ON customers_masked TO analyst;

-- Bảng mới của owner coffee: analyst tự có SELECT. Bảng mới có PII → REVOKE tay, đưa vào checklist review schema.
ALTER DEFAULT PRIVILEGES FOR ROLE coffee IN SCHEMA public GRANT SELECT ON TABLES TO analyst;

-- ── Kiểm tra ───────────────────────────────────────────────────────────────────
SET ROLE analyst;
SELECT count(*) AS n_txn FROM pos_transactions;                       -- OK
SELECT customer_id, city, tier FROM customers LIMIT 2;                -- OK
SELECT customer_id, phone_masked FROM customers_masked LIMIT 2;       -- OK: *******580
SELECT phone FROM customers LIMIT 1;                                  -- ERROR: permission denied for table customers
SELECT * FROM customers LIMIT 1;                                      -- ERROR: * chạm vào cột bị cấm
RESET ROLE;

-- Đăng nhập thật từ host để chắc chắn (mật khẩu: analyst):
--   docker compose exec postgres psql -U analyst -d coffee_pos -c "SELECT email FROM customers LIMIT 1"
