-- Lab #06 — star schema cho Phin Phố trên DuckDB, load thẳng từ data/samples/*.csv.
-- Chạy: uv run python videos/06-data-modeling/lab/load_star.py videos/06-data-modeling/lab/star_schema.sql
--
-- File này CHẠY ĐƯỢC ngay (DDL hợp lệ) nhưng còn thiếu cột và fct_sales rỗng. Điền từng TODO,
-- chạy lại, so số dòng với README "Kết quả mong đợi". Đích: fct_sales 4 302 dòng, revenue 240 469 000.
-- Đây chính là mô hình platform/dbt/models/marts sinh ra ở video #12 — hãy mở các file .sql ở đó để đối chiếu.

SET TimeZone = 'Asia/Ho_Chi_Minh';

-- ═══════════════════════════════════════════════════════════════════════════════
-- 0. Nguồn thô — 5 bảng OLTP, giữ nguyên (vai trò bronze). Phần này đã xong.
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE raw_stores        AS SELECT * FROM read_csv('data/samples/stores.csv');
CREATE OR REPLACE TABLE raw_menu_items    AS SELECT * FROM read_csv('data/samples/menu_items.csv');
CREATE OR REPLACE TABLE raw_customers     AS SELECT * FROM read_csv('data/samples/customers.csv', all_varchar = true);
CREATE OR REPLACE TABLE raw_transactions  AS SELECT * FROM read_csv('data/samples/pos_transactions.csv', all_varchar = true, nullstr = '');
CREATE OR REPLACE TABLE raw_line_items    AS SELECT * FROM read_csv('data/samples/pos_line_items.csv');

-- ═══════════════════════════════════════════════════════════════════════════════
-- 1. Làm sạch — 4 rule của silver.py: duplicate, null_store, tz_drift, negative_qty. Bạn đã viết ở #03.
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW txn_clean AS
SELECT
    transaction_id,
    store_id,
    -- TODO 1: ordered_at_utc TIMESTAMPTZ — thiếu '+07:00' thì nối vào (replace ' ' bằng 'T' trước)
    ts::VARCHAR AS ordered_at_utc,
    channel,
    payment_method,
    customer_id,
    total_amount_vnd::BIGINT AS total_amount_vnd,
    discount_vnd::BIGINT     AS discount_vnd,
    is_rainy_day::BOOLEAN    AS is_rainy_day
FROM raw_transactions
WHERE store_id IS NOT NULL
-- TODO 2: dedup bằng QUALIFY row_number() OVER (PARTITION BY transaction_id ORDER BY ts) = 1
;

CREATE OR REPLACE VIEW lines_clean AS
SELECT l.*
FROM raw_line_items l
JOIN txn_clean t USING (transaction_id)
-- TODO 3: WHERE quantity > 0
;

-- ═══════════════════════════════════════════════════════════════════════════════
-- 2. Dimensions
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE dim_store (
    store_id     VARCHAR PRIMARY KEY,
    store_name   VARCHAR NOT NULL,
    city         VARCHAR NOT NULL,
    district     VARCHAR NOT NULL,
    opened_date  DATE    NOT NULL,
    seats        INTEGER NOT NULL,
    tier         DECIMAL(3, 2) NOT NULL
    -- TODO 4: thêm store_class VARCHAR NOT NULL: tier >= 1.2 → 'flagship', >= 0.9 → 'standard', còn lại 'kiosk'
    --         (thuộc tính SUY RA chỉ có ở dim — OLTP không có cột này; so với dbt dim_store.sql)
);
INSERT INTO dim_store
SELECT store_id, store_name, city, district, opened_date, seats, tier
FROM raw_stores;

CREATE OR REPLACE TABLE dim_menu_item (
    item_id     VARCHAR PRIMARY KEY,
    item_name   VARCHAR NOT NULL,
    category    VARCHAR NOT NULL,
    price_vnd   INTEGER NOT NULL,
    cost_vnd    INTEGER NOT NULL
    -- TODO 5: thêm margin_vnd INTEGER và margin_pct DECIMAL(5, 1) (round((price - cost) * 100.0 / price, 1))
);
INSERT INTO dim_menu_item
SELECT item_id, item_name, category, price_vnd, cost_vnd
FROM raw_menu_items;

-- TODO 6: dim_customer (customer_id PK, city, joined_date DATE, tier, phone_masked).
--   KHÔNG mang full_name / phone / email vào gold. phone_masked giữ 3 số cuối:
--   repeat('*', greatest(length(phone) - 3, 0)) || right(phone, 3)   (giống macro mask_pii của dbt)
CREATE OR REPLACE TABLE dim_customer (
    customer_id   VARCHAR PRIMARY KEY
);
INSERT INTO dim_customer SELECT customer_id FROM raw_customers;

-- TODO 7: dim_date 2025-01-01 → 2027-12-31 (1 095 dòng) với year, month, iso_weekday, is_weekend,
--   is_tet_season (mùng 1 − 7 … mùng 1 + 5), is_tet_holiday (mùng 1 … mùng 1 + 2).
--   Mùng 1: 2025-01-29, 2026-02-17, 2027-02-06. Gợi ý: range(DATE '2025-01-01', DATE '2028-01-01', INTERVAL 1 DAY)
--   và EXISTS (SELECT 1 FROM tet WHERE d BETWEEN mung1 - 7 AND mung1 + 5). Đối chiếu dbt dim_date.sql.
CREATE OR REPLACE TABLE dim_date (
    date_day  DATE PRIMARY KEY
);
INSERT INTO dim_date
SELECT d::DATE FROM range(DATE '2025-01-01', DATE '2028-01-01', INTERVAL 1 DAY) t(d);

-- ═══════════════════════════════════════════════════════════════════════════════
-- 3. Fact — grain = 1 LINE ITEM. Mọi measure additive. channel/payment_method/size là degenerate dimension.
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE fct_sales (
    line_id          VARCHAR PRIMARY KEY,
    transaction_id   VARCHAR NOT NULL,
    business_date    DATE    NOT NULL REFERENCES dim_date (date_day),
    ordered_at_utc   TIMESTAMPTZ NOT NULL,
    store_id         VARCHAR NOT NULL REFERENCES dim_store (store_id),
    customer_id      VARCHAR REFERENCES dim_customer (customer_id),
    item_id          VARCHAR NOT NULL REFERENCES dim_menu_item (item_id),
    channel          VARCHAR NOT NULL,
    payment_method   VARCHAR NOT NULL,
    size             VARCHAR NOT NULL,
    quantity         INTEGER NOT NULL,
    unit_price_vnd   INTEGER NOT NULL,
    line_total_vnd   INTEGER NOT NULL,
    line_cost_vnd    INTEGER NOT NULL,     -- quantity × cost_vnd của món
    line_margin_vnd  INTEGER NOT NULL,     -- line_total − line_cost
    is_rainy_day     BOOLEAN NOT NULL
);
-- TODO 8: INSERT INTO fct_sales SELECT ... FROM lines_clean l JOIN txn_clean t USING (transaction_id) JOIN dim_menu_item m USING (item_id)
--   business_date = (t.ordered_at_utc AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE
--   Xoá dòng WHERE false bên dưới khi làm xong. Mong đợi: 4 302 dòng, sum(line_total_vnd) = 240 469 000.
INSERT INTO fct_sales
SELECT NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
WHERE false;

-- ═══════════════════════════════════════════════════════════════════════════════
-- 4. Câu hỏi nghiệp vụ: doanh thu / margin theo ngày × cửa hàng × món × kênh
-- ═══════════════════════════════════════════════════════════════════════════════
-- TODO 9: CREATE OR REPLACE VIEW rpt_revenue_margin AS SELECT business_date, is_tet_season, city, store_name,
--   category, item_name, channel, sum(quantity) qty, sum(line_total_vnd) revenue_vnd, sum(line_margin_vnd) margin_vnd
--   FROM fct_sales JOIN dim_store USING (store_id) JOIN dim_menu_item USING (item_id) JOIN dim_date ON date_day = business_date
--   GROUP BY ALL.   Để ý: 1 fact JOIN 3 dim, không CTE, không window — đó là lý do tồn tại của star schema.
