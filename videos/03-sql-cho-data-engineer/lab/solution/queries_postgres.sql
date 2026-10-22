-- Lời giải #03 — cùng 6 bài, chạy trên Postgres (bảng đã seed bởi make up).
-- Chạy: docker compose exec -T postgres psql -U coffee -d coffee_pos < videos/03-sql-cho-data-engineer/lab/solution/queries_postgres.sql
-- Khác DuckDB ở 3 chỗ: (a) ts đã là TIMESTAMPTZ nên business_date phải đổi về giờ VN;
-- (b) không có QUALIFY/EXCLUDE; (c) tz_drift phải đọc lại file CSV thô mới bắt được (xem Q5).
\timing on

-- (1) DEDUP ─────────────────────────────────────────────────────────────────────
-- Q1a 5 dòng
SELECT transaction_id, count(*) AS copies
FROM pos_transactions
GROUP BY 1 HAVING count(*) > 1
ORDER BY 1;

-- Q1b 2481 → 2476
WITH ranked AS (
    SELECT *, row_number() OVER (PARTITION BY transaction_id ORDER BY ts) AS rn
    FROM pos_transactions
)
SELECT (SELECT count(*) FROM pos_transactions) AS rows_before, count(*) AS rows_after
FROM ranked WHERE rn = 1;

-- Q1c TEMP VIEW sống trong phiên psql này. business_date theo giờ Việt Nam.
CREATE TEMP VIEW txn_clean AS
SELECT transaction_id, store_id, ts, channel, payment_method, customer_id, staff_id,
       total_amount_vnd AS amount_vnd, discount_vnd, is_rainy_day,
       (ts AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS business_date
FROM (
    SELECT *, row_number() OVER (PARTITION BY transaction_id ORDER BY ts) AS rn
    FROM pos_transactions
    WHERE store_id IS NOT NULL
) r
WHERE rn = 1;

-- Q1d Số sẽ KHÁC DuckDB một chút: hoá đơn HN07-20260214-00014 (21:06 ngày 14, thiếu offset) bị Postgres
--     hiểu là UTC → 04:06 ngày 15. Ngày 14 thiếu 172 000, ngày 15 thừa 172 000. Đó là tz_drift bằng xương bằng thịt.
SELECT business_date, count(*) AS transactions, sum(amount_vnd) AS revenue_vnd
FROM txn_clean GROUP BY 1 ORDER BY 1;

-- (2) RUNNING TOTAL ─────────────────────────────────────────────────────────────
-- Q2 180 dòng
WITH daily AS (
    SELECT store_id, business_date, sum(amount_vnd) AS revenue_vnd
    FROM txn_clean GROUP BY 1, 2
)
SELECT store_id, business_date, revenue_vnd,
       sum(revenue_vnd) OVER (PARTITION BY store_id ORDER BY business_date) AS running_revenue_vnd
FROM daily ORDER BY 1, 2;

-- (3) TOP 3 ─────────────────────────────────────────────────────────────────────
-- Q3 90 dòng
WITH item_rev AS (
    SELECT t.store_id, l.item_id, sum(l.line_total_vnd) AS revenue_vnd, sum(l.quantity) AS qty
    FROM pos_line_items l
    JOIN txn_clean t USING (transaction_id)
    WHERE l.quantity > 0
    GROUP BY 1, 2
),
ranked AS (
    SELECT *, dense_rank() OVER (PARTITION BY store_id ORDER BY revenue_vnd DESC) AS rk FROM item_rev
)
SELECT r.store_id, r.rk, r.item_id, m.item_name, r.revenue_vnd, r.qty
FROM ranked r JOIN menu_items m USING (item_id)
WHERE rk <= 3 ORDER BY 1, 2;

-- (4) LỊCH ──────────────────────────────────────────────────────────────────────
-- Q4a 24 dòng. extract(hour) phải lấy theo giờ VN, không phải UTC.
SELECT h.hour, count(t.transaction_id) AS transactions
FROM generate_series(0, 23) AS h(hour)
LEFT JOIN txn_clean t
       ON t.business_date = DATE '2026-02-15'
      AND extract(hour FROM t.ts AT TIME ZONE 'Asia/Ho_Chi_Minh') = h.hour
GROUP BY 1 ORDER BY 1;

-- Q4b CTE đệ quy, 6 dòng
WITH RECURSIVE days(d) AS (
    SELECT DATE '2026-02-14'
    UNION ALL
    SELECT d + 1 FROM days WHERE d < DATE '2026-02-19'
)
SELECT d AS business_date, to_char(d, 'Dy') AS weekday, d = DATE '2026-02-17' AS is_tet_mung_1
FROM days ORDER BY 1;

-- (5) TZ_DRIFT ──────────────────────────────────────────────────────────────────
-- Q5a Trong bảng pos_transactions, ts đã bị parse → không còn dấu vết. Phải đọc lại CSV thô (mount tại /opt/samples)
--     vào bảng tạm toàn TEXT — đúng tinh thần bronze: giữ chuỗi gốc.
CREATE TEMP TABLE raw_txn (
    transaction_id text, store_id text, ts text, channel text, payment_method text,
    customer_id text, staff_id text, total_amount_vnd text, discount_vnd text, is_rainy_day text
);
COPY raw_txn FROM '/opt/samples/pos_transactions.csv' WITH (FORMAT csv, HEADER true, NULL '');

-- Q5b 11 dòng
SELECT transaction_id, store_id, ts FROM raw_txn WHERE ts !~ '\+07:00$' ORDER BY ts;

-- Q5c Hậu quả trong bảng thật: 11 hoá đơn này đang lệch +7 giờ. Cột fixed là giờ đúng.
SELECT r.transaction_id,
       r.ts                                             AS ts_raw,
       p.ts AT TIME ZONE 'Asia/Ho_Chi_Minh'             AS stored_shown_in_vn,
       (r.ts || '+07:00')::timestamptz AT TIME ZONE 'Asia/Ho_Chi_Minh' AS fixed_vn_local
FROM raw_txn r
JOIN pos_transactions p USING (transaction_id)
WHERE r.ts !~ '\+07:00$'
ORDER BY r.ts;

-- Q5d Không có CSV thì sao? Heuristic: hoá đơn ngoài giờ mở cửa (23h–5h). Chỉ bắt được 2/11 dòng — heuristic không thay được dữ liệu thô.
SELECT transaction_id, ts AT TIME ZONE 'Asia/Ho_Chi_Minh' AS vn_time
FROM pos_transactions
WHERE extract(hour FROM ts AT TIME ZONE 'Asia/Ho_Chi_Minh') IN (23, 0, 1, 2, 3, 4)
ORDER BY ts;

-- (6) QUERY PLAN ────────────────────────────────────────────────────────────────
-- Q6 Đọc từ trong ra: Seq Scan → Sort (transaction_id, ts) → WindowAgg → Subquery Scan (Filter rn = 1) → Aggregate.
--    Sort là chỗ tốn nhất: window function cần dữ liệu đã sắp theo PARTITION BY, ORDER BY. Với 500 k dòng
--    sort có thể tràn ra đĩa ("Sort Method: external merge") — đó là lúc bạn cần index (transaction_id, ts) hoặc work_mem lớn hơn.
EXPLAIN (ANALYZE, BUFFERS)
WITH ranked AS (
    SELECT *, row_number() OVER (PARTITION BY transaction_id ORDER BY ts) AS rn
    FROM pos_transactions
)
SELECT count(*) FROM ranked WHERE rn = 1;
