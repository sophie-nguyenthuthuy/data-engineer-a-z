-- Lời giải #03 — SQL cho Data Engineer, bản DuckDB đọc thẳng data/samples/*.csv (không cần Docker).
-- Chạy: uv run python videos/03-sql-cho-data-engineer/lab/run_sql.py videos/03-sql-cho-data-engineer/lab/solution/queries.sql
-- Quy ước: mỗi câu bắt đầu bằng dòng "-- Qn". Đường dẫn tương đối so với root repo.

-- Q0a nạp POS thô: all_varchar=true để KHÔNG ai đoán kiểu — ts phải còn là chuỗi để câu Q5 bắt được tz_drift
CREATE OR REPLACE TABLE raw_txn AS
SELECT * FROM read_csv('data/samples/pos_transactions.csv', all_varchar=true, nullstr='');

-- Q0b line items (để DuckDB tự suy kiểu: quantity, line_total_vnd là số)
CREATE OR REPLACE TABLE raw_lines AS
SELECT * FROM read_csv('data/samples/pos_line_items.csv');

-- Q0c menu
CREATE OR REPLACE TABLE menu_items AS
SELECT * FROM read_csv('data/samples/menu_items.csv');

-- ═══════════════════════════════════════════════════════════════════════════════
-- (1) DEDUP bằng ROW_NUMBER — 5 transaction_id xuất hiện 2 lần (retry khi mạng chập chờn)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Q1a các hoá đơn bị trùng: 5 dòng
SELECT transaction_id, count(*) AS copies
FROM raw_txn
GROUP BY 1
HAVING count(*) > 1
ORDER BY 1;

-- Q1b dedup: giữ dòng đầu tiên của mỗi transaction_id. ORDER BY ts để "đầu tiên" có nghĩa xác định.
--     2 481 → 2 476 dòng. (So với silver.py: clean_transactions làm đúng việc này bằng Polars.)
WITH ranked AS (
    SELECT *, row_number() OVER (PARTITION BY transaction_id ORDER BY ts) AS rn
    FROM raw_txn
)
SELECT (SELECT count(*) FROM raw_txn) AS rows_before,
       count(*)                       AS rows_after
FROM ranked
WHERE rn = 1;

-- Q1c lưu thành view để các câu sau dùng. Bỏ luôn store_id NULL (8 dòng — không gán được cho cửa hàng nào).
--     business_date = 10 ký tự đầu của ts: ts trong file là giờ LOCAL dù có hay không có +07:00, nên cắt chuỗi là đúng.
CREATE OR REPLACE VIEW txn_clean AS
SELECT * EXCLUDE (rn), substr(ts, 1, 10) AS business_date, total_amount_vnd::BIGINT AS amount_vnd
FROM (
    SELECT *, row_number() OVER (PARTITION BY transaction_id ORDER BY ts) AS rn
    FROM raw_txn
    WHERE store_id IS NOT NULL
)
WHERE rn = 1;

-- Q1d kiểm tra: số hoá đơn & doanh thu theo ngày sau khi làm sạch — khớp silver của coffee-ingest
--     14/02: 766 / 73 397 000 · 15/02: 792 / 76 939 000 · 16/02: 730 / 70 254 000 · 17/02: 59 / 5 549 000
SELECT business_date, count(*) AS transactions, sum(amount_vnd) AS revenue_vnd
FROM txn_clean
GROUP BY 1
ORDER BY 1;

-- ═══════════════════════════════════════════════════════════════════════════════
-- (2) RUNNING TOTAL doanh thu theo ngày, từng cửa hàng — window SUM với ORDER BY
-- ═══════════════════════════════════════════════════════════════════════════════

-- Q2 180 dòng (30 cửa hàng × 6 ngày). HN01: 1 111 000 → 2 693 000 → 3 835 000 → 3 925 000 → 4 075 000 → 4 110 000
WITH daily AS (
    SELECT store_id, business_date, sum(amount_vnd) AS revenue_vnd
    FROM txn_clean
    GROUP BY 1, 2
)
SELECT
    store_id,
    business_date,
    revenue_vnd,
    sum(revenue_vnd) OVER (PARTITION BY store_id ORDER BY business_date) AS running_revenue_vnd
FROM daily
ORDER BY 1, 2;

-- ═══════════════════════════════════════════════════════════════════════════════
-- (3) TOP 3 món mỗi cửa hàng — DENSE_RANK (hoà điểm thì cùng hạng, không bỏ số)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Q3 90 dòng. HN01: TE01 471 000 · TE02 452 000 · CF02 380 000. SG14: CF03 1 593 000 · CF02 1 540 000 · TE01 1 114 000
--    quantity > 0: bỏ 10 dòng hoàn hàng sai (negative_qty). JOIN txn_clean: bỏ line của hoá đơn trùng / không store.
WITH item_rev AS (
    SELECT t.store_id, l.item_id, sum(l.line_total_vnd) AS revenue_vnd, sum(l.quantity) AS qty
    FROM raw_lines l
    JOIN txn_clean t USING (transaction_id)
    WHERE l.quantity > 0
    GROUP BY 1, 2
),
ranked AS (
    SELECT *, dense_rank() OVER (PARTITION BY store_id ORDER BY revenue_vnd DESC) AS rk
    FROM item_rev
)
SELECT r.store_id, r.rk, r.item_id, m.item_name, r.revenue_vnd, r.qty
FROM ranked r
JOIN menu_items m USING (item_id)
WHERE rk <= 3
ORDER BY 1, 2;

-- ═══════════════════════════════════════════════════════════════════════════════
-- (4) LẬP LỊCH bằng generate_series / CTE đệ quy — để nhìn thấy khoảng trống (giờ không có hoá đơn)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Q4a 24 dòng cho ngày 15/02: giờ 0–4 và 23 có 0 hoá đơn (quán đóng cửa) — GROUP BY thường sẽ KHÔNG hiện các giờ này
SELECT h.hour, count(t.transaction_id) AS transactions
FROM generate_series(0, 23) AS h(hour)
LEFT JOIN txn_clean t
       ON t.business_date = '2026-02-15'
      AND substr(t.ts, 12, 2)::INT = h.hour
GROUP BY 1
ORDER BY 1;

-- Q4b cùng ý với CTE đệ quy (cách làm khi engine không có generate_series): lịch 6 ngày 14→19/02
WITH RECURSIVE days(d) AS (
    SELECT DATE '2026-02-14'
    UNION ALL
    SELECT d + INTERVAL 1 DAY FROM days WHERE d < DATE '2026-02-19'
)
SELECT d::DATE AS business_date, dayname(d) AS weekday, d::DATE = DATE '2026-02-17' AS is_tet_mung_1
FROM days
ORDER BY 1;

-- ═══════════════════════════════════════════════════════════════════════════════
-- (5) TZ_DRIFT — ts thiếu +07:00. Chỉ bắt được khi ts còn là CHUỖI (all_varchar=true ở Q0a)
-- ═══════════════════════════════════════════════════════════════════════════════

-- Q5a 11 dòng. Dấu hiệu: không kết thúc bằng offset, và dùng dấu cách thay cho 'T'
SELECT transaction_id, store_id, ts
FROM raw_txn
WHERE ts NOT LIKE '%+07:00'
ORDER BY ts;

-- Q5b hậu quả nếu nạp thẳng vào TIMESTAMPTZ (bị coi là UTC): lệch +7 giờ, 21:06 ngày 14 trở thành 04:06 ngày 15.
--     Cách sửa = parse_vn_timestamp trong platform/ingestion/src/coffee_ingestion/silver.py: thiếu offset ⇒ nối '+07:00'.
SELECT
    transaction_id,
    ts                                                                             AS ts_raw,
    (replace(ts, ' ', 'T') || '+00:00')::TIMESTAMPTZ AT TIME ZONE 'Asia/Ho_Chi_Minh' AS wrong_if_read_as_utc,
    (replace(ts, ' ', 'T') || '+07:00')::TIMESTAMPTZ AT TIME ZONE 'Asia/Ho_Chi_Minh' AS fixed_vn_local
FROM raw_txn
WHERE ts NOT LIKE '%+07:00'
ORDER BY ts;

-- ═══════════════════════════════════════════════════════════════════════════════
-- (6) QUERY PLAN — EXPLAIN ANALYZE của câu dedup
-- ═══════════════════════════════════════════════════════════════════════════════

-- Q6 Đọc từ DƯỚI lên: Table Scan (READ_CSV, Projections: chỉ cột cần) → ... → kết quả.
--    Để ý: DuckDB thấy ta chỉ cần count(*) nên đổi WINDOW thành HASH GROUP BY — optimizer được phép viết lại câu của bạn.
--    Thử EXPLAIN ANALYZE SELECT * FROM txn_clean LIMIT 5 để thấy operator WINDOW thật.
EXPLAIN ANALYZE
WITH ranked AS (
    SELECT *, row_number() OVER (PARTITION BY transaction_id ORDER BY ts) AS rn
    FROM raw_txn
)
SELECT count(*) FROM ranked WHERE rn = 1;
