-- Lab #03 — cùng 6 bài của queries.sql, chạy trên Postgres (bảng đã seed bởi make up).
-- Chạy: docker compose exec -T postgres psql -U coffee -d coffee_pos < videos/03-sql-cho-data-engineer/lab/queries_postgres.sql
-- Làm xong bản DuckDB trước, rồi "dịch" sang đây. Khác nhau ở 3 chỗ:
--   (a) ts đã là TIMESTAMPTZ → business_date = (ts AT TIME ZONE 'Asia/Ho_Chi_Minh')::date
--   (b) Postgres không có QUALIFY / EXCLUDE → dùng subquery + liệt kê cột
--   (c) tz_drift không còn dấu vết trong bảng → phải COPY lại CSV thô vào bảng tạm (xem Q5a)
\timing on

-- Q1a TODO: transaction_id bị trùng (5 dòng)
SELECT 'TODO';

-- Q1b TODO: rows_before, rows_after (2481 → 2476)
SELECT 'TODO';

-- Q1c TODO: CREATE TEMP VIEW txn_clean AS ... (bỏ store_id NULL, rn = 1, thêm business_date theo giờ VN, amount_vnd)
SELECT 'TODO';

-- Q1d TODO: hoá đơn & doanh thu theo business_date. So với DuckDB: ngày 14 và 15 lệch nhau 172 000 — vì sao? (gợi ý: Q5)
SELECT 'TODO';

-- Q2 TODO: running total per store (180 dòng)
SELECT 'TODO';

-- Q3 TODO: top 3 món mỗi cửa hàng bằng dense_rank (90 dòng), JOIN menu_items
SELECT 'TODO';

-- Q4a TODO: 24 giờ của ngày 2026-02-15, LEFT JOIN generate_series(0, 23). Giờ lấy bằng extract(hour FROM ts AT TIME ZONE 'Asia/Ho_Chi_Minh')
SELECT 'TODO';

-- Q4b TODO: WITH RECURSIVE days(d) ... lịch 14→19/02, weekday = to_char(d, 'Dy')
SELECT 'TODO';

-- Q5a Đọc lại CSV thô (đã mount sẵn tại /opt/samples trong container) vào bảng tạm toàn TEXT:
CREATE TEMP TABLE raw_txn (
    transaction_id text, store_id text, ts text, channel text, payment_method text,
    customer_id text, staff_id text, total_amount_vnd text, discount_vnd text, is_rainy_day text
);
COPY raw_txn FROM '/opt/samples/pos_transactions.csv' WITH (FORMAT csv, HEADER true, NULL '');

-- Q5b TODO: dòng ts không kết thúc bằng +07:00 (regex: ts !~ '\+07:00$'). 11 dòng.
SELECT 'TODO';

-- Q5c TODO: JOIN raw_txn với pos_transactions theo transaction_id, hiển thị ts_raw, p.ts AT TIME ZONE 'Asia/Ho_Chi_Minh'
--     và (r.ts || '+07:00')::timestamptz AT TIME ZONE 'Asia/Ho_Chi_Minh'. Cột nào đúng?
SELECT 'TODO';

-- Q6 TODO: EXPLAIN (ANALYZE, BUFFERS) câu Q1b. Tìm node "WindowAgg" và "Sort". Sort Method là gì?
SELECT 'TODO';
