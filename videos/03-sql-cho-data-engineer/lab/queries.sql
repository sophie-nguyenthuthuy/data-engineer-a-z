-- Lab #03 — SQL cho Data Engineer, bản DuckDB đọc thẳng data/samples/*.csv (không cần Docker).
-- Chạy: uv run python videos/03-sql-cho-data-engineer/lab/run_sql.py videos/03-sql-cho-data-engineer/lab/queries.sql
-- Mỗi câu bắt đầu bằng dòng "-- Qn". Câu còn chữ TODO sẽ bị bỏ qua, nên hãy xoá chữ TODO khi làm xong.
-- Đường dẫn tương đối so với root repo.

-- Q0a nạp POS thô. all_varchar=true: KHÔNG để DuckDB đoán kiểu — ts phải còn là chuỗi để câu Q5 bắt được tz_drift
CREATE OR REPLACE TABLE raw_txn AS
SELECT * FROM read_csv('data/samples/pos_transactions.csv', all_varchar=true, nullstr='');

-- Q0b line items (để DuckDB tự suy kiểu: quantity, line_total_vnd là số)
CREATE OR REPLACE TABLE raw_lines AS
SELECT * FROM read_csv('data/samples/pos_line_items.csv');

-- Q0c menu
CREATE OR REPLACE TABLE menu_items AS
SELECT * FROM read_csv('data/samples/menu_items.csv');

-- ═══════════════════════════════════════════════════════════════════════════════
-- (1) DEDUP bằng ROW_NUMBER — có transaction_id xuất hiện 2 lần (retry khi mạng chập chờn)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Q1a Liệt kê transaction_id bị trùng và số bản sao.
--     TODO: GROUP BY transaction_id ... HAVING count(*) > 1. Kết quả mong đợi: 5 dòng.
SELECT 'TODO';

-- Q1b Dedup: với mỗi transaction_id chỉ giữ 1 dòng. Trả về rows_before, rows_after.
--     TODO: CTE ranked AS (SELECT *, row_number() OVER (PARTITION BY transaction_id ORDER BY ts) AS rn FROM raw_txn)
--           rồi count(*) WHERE rn = 1. Mong đợi: 2481 → 2476.
SELECT 'TODO';

-- Q1c Lưu kết quả dedup thành VIEW txn_clean để các câu sau dùng:
--     - bỏ dòng store_id IS NULL (không gán được cho cửa hàng nào)
--     - thêm cột business_date = substr(ts, 1, 10) (ts trong file là giờ LOCAL dù có hay không có +07:00)
--     - thêm cột amount_vnd = total_amount_vnd::BIGINT
--     TODO: CREATE OR REPLACE VIEW txn_clean AS SELECT * EXCLUDE (rn), ... FROM (...) WHERE rn = 1
SELECT 'TODO';

-- Q1d Kiểm tra: số hoá đơn và doanh thu theo business_date từ txn_clean.
--     Mong đợi 15/02: 792 hoá đơn, 76 939 000 VND (khớp silver của coffee-ingest).
--     TODO
SELECT 'TODO';

-- ═══════════════════════════════════════════════════════════════════════════════
-- (2) RUNNING TOTAL doanh thu theo ngày, từng cửa hàng
-- ═══════════════════════════════════════════════════════════════════════════════

-- Q2 Cột: store_id, business_date, revenue_vnd, running_revenue_vnd. Mong đợi 180 dòng (30 cửa hàng × 6 ngày).
--    TODO: CTE daily (GROUP BY store_id, business_date) rồi
--          sum(revenue_vnd) OVER (PARTITION BY store_id ORDER BY business_date)
SELECT 'TODO';

-- ═══════════════════════════════════════════════════════════════════════════════
-- (3) TOP 3 món mỗi cửa hàng theo doanh thu — DENSE_RANK
-- ═══════════════════════════════════════════════════════════════════════════════

-- Q3 Cột: store_id, rk, item_id, item_name, revenue_vnd, qty. Mong đợi 90 dòng.
--    Điều kiện: quantity > 0 (bỏ hoàn hàng sai) và JOIN txn_clean (bỏ line của hoá đơn trùng / không store).
--    TODO: CTE item_rev (GROUP BY store_id, item_id) → dense_rank() OVER (PARTITION BY store_id ORDER BY revenue_vnd DESC)
--          → WHERE rk <= 3 → JOIN menu_items để lấy item_name
--    Câu hỏi phụ: nếu dùng rank() hoặc row_number() thay cho dense_rank() thì khi nào kết quả khác?
SELECT 'TODO';

-- ═══════════════════════════════════════════════════════════════════════════════
-- (4) LẬP LỊCH bằng generate_series / CTE đệ quy
-- ═══════════════════════════════════════════════════════════════════════════════

-- Q4a Số hoá đơn theo GIỜ của ngày 2026-02-15, đủ 24 dòng kể cả giờ không có hoá đơn nào.
--     TODO: FROM generate_series(0, 23) AS h(hour) LEFT JOIN txn_clean t ON t.business_date = '2026-02-15'
--           AND substr(t.ts, 12, 2)::INT = h.hour. Mong đợi: giờ 0–4 và 23 có 0 hoá đơn.
SELECT 'TODO';

-- Q4b Cùng ý bằng CTE đệ quy: lịch 6 ngày 14→19/02/2026, cột business_date, weekday (dayname), is_tet_mung_1.
--     TODO: WITH RECURSIVE days(d) AS (SELECT DATE '2026-02-14' UNION ALL SELECT d + INTERVAL 1 DAY FROM days WHERE d < ...)
SELECT 'TODO';

-- ═══════════════════════════════════════════════════════════════════════════════
-- (5) TZ_DRIFT — ts thiếu +07:00
-- ═══════════════════════════════════════════════════════════════════════════════

-- Q5a Liệt kê các dòng ts KHÔNG kết thúc bằng '+07:00'. Mong đợi 11 dòng.
--     TODO: WHERE ts NOT LIKE '%+07:00'
SELECT 'TODO';

-- Q5b Cho thấy hậu quả: với các dòng ở Q5a, hiển thị
--     wrong_if_read_as_utc = (replace(ts, ' ', 'T') || '+00:00')::TIMESTAMPTZ AT TIME ZONE 'Asia/Ho_Chi_Minh'
--     fixed_vn_local       = (replace(ts, ' ', 'T') || '+07:00')::TIMESTAMPTZ AT TIME ZONE 'Asia/Ho_Chi_Minh'
--     Đây chính là logic của parse_vn_timestamp trong platform/ingestion/src/coffee_ingestion/silver.py.
--     TODO
SELECT 'TODO';

-- ═══════════════════════════════════════════════════════════════════════════════
-- (6) QUERY PLAN
-- ═══════════════════════════════════════════════════════════════════════════════

-- Q6 EXPLAIN ANALYZE câu Q1b. Đọc plan từ DƯỚI lên. Operator nào tốn thời gian nhất? Cột nào được đọc từ bảng (Projections)?
--    TODO: viết EXPLAIN ANALYZE <câu Q1b>
SELECT 'TODO';
