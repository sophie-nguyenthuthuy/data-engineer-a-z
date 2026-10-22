-- Lời giải #06 — star schema cho Phin Phố trên DuckDB, load thẳng từ data/samples/*.csv.
-- Chạy: uv run python videos/06-data-modeling/lab/load_star.py videos/06-data-modeling/lab/solution/star_schema.sql
--
-- Đây chính là mô hình mà platform/dbt/models/marts sinh ra ở video #12 (dim_store, dim_menu_item,
-- dim_customer, dim_date, fct_sales). Ở đây viết tay bằng DDL để hiểu từng cột; dbt sẽ làm việc này
-- bằng SELECT + test + docs, và đọc từ silver Parquet thay vì CSV.

SET TimeZone = 'Asia/Ho_Chi_Minh';

-- ═══════════════════════════════════════════════════════════════════════════════
-- 0. Nguồn thô — 5 bảng OLTP, giữ nguyên (vai trò bronze)
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE raw_stores        AS SELECT * FROM read_csv('data/samples/stores.csv');
CREATE OR REPLACE TABLE raw_menu_items    AS SELECT * FROM read_csv('data/samples/menu_items.csv');
CREATE OR REPLACE TABLE raw_customers     AS SELECT * FROM read_csv('data/samples/customers.csv', all_varchar = true);
CREATE OR REPLACE TABLE raw_transactions  AS SELECT * FROM read_csv('data/samples/pos_transactions.csv', all_varchar = true, nullstr = '');
CREATE OR REPLACE TABLE raw_line_items    AS SELECT * FROM read_csv('data/samples/pos_line_items.csv');

-- ═══════════════════════════════════════════════════════════════════════════════
-- 1. Làm sạch — đúng 4 rule của platform/ingestion/src/coffee_ingestion/silver.py
--    duplicate → giữ dòng đầu; null_store → bỏ; tz_drift → thiếu offset = giờ VN; negative_qty → bỏ
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW txn_clean AS
SELECT
    transaction_id,
    store_id,
    (CASE WHEN ts LIKE '%+07:00' THEN ts ELSE replace(ts, ' ', 'T') || '+07:00' END)::TIMESTAMPTZ AS ordered_at_utc,
    channel,
    payment_method,
    customer_id,
    total_amount_vnd::BIGINT AS total_amount_vnd,
    discount_vnd::BIGINT     AS discount_vnd,
    is_rainy_day::BOOLEAN    AS is_rainy_day
FROM raw_transactions
WHERE store_id IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY transaction_id ORDER BY ts) = 1;

CREATE OR REPLACE VIEW lines_clean AS
SELECT l.*
FROM raw_line_items l
JOIN txn_clean t USING (transaction_id)   -- bỏ line của hoá đơn không hợp lệ (orphan)
WHERE l.quantity > 0;                      -- bỏ negative_qty

-- ═══════════════════════════════════════════════════════════════════════════════
-- 2. Dimensions — mỗi dim có khoá tự nhiên làm PK (chuỗi nhỏ, ổn định). Surrogate key int để dành
--    cho khi dim có SCD2 (xem data_vault.sql và snapshots/menu_price_snapshot.sql của dbt).
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE dim_store (
    store_id     VARCHAR PRIMARY KEY,
    store_name   VARCHAR NOT NULL,
    city         VARCHAR NOT NULL,
    district     VARCHAR NOT NULL,
    opened_date  DATE    NOT NULL,
    seats        INTEGER NOT NULL,
    tier         DECIMAL(3, 2) NOT NULL,
    store_class  VARCHAR NOT NULL        -- thuộc tính suy ra, chỉ có ở dim, không có ở OLTP
);
INSERT INTO dim_store
SELECT store_id, store_name, city, district, opened_date, seats, tier,
       CASE WHEN tier >= 1.2 THEN 'flagship' WHEN tier >= 0.9 THEN 'standard' ELSE 'kiosk' END
FROM raw_stores;

CREATE OR REPLACE TABLE dim_menu_item (
    item_id     VARCHAR PRIMARY KEY,
    item_name   VARCHAR NOT NULL,
    category    VARCHAR NOT NULL,
    price_vnd   INTEGER NOT NULL,
    cost_vnd    INTEGER NOT NULL,
    margin_vnd  INTEGER NOT NULL,
    margin_pct  DECIMAL(5, 1) NOT NULL
);
INSERT INTO dim_menu_item
SELECT item_id, item_name, category, price_vnd, cost_vnd,
       price_vnd - cost_vnd,
       round((price_vnd - cost_vnd) * 100.0 / price_vnd, 1)
FROM raw_menu_items;

-- PII (full_name, phone, email) KHÔNG đi vào gold. Giữ 3 số cuối phone để đối soát — y hệt macro mask_pii của dbt.
CREATE OR REPLACE TABLE dim_customer (
    customer_id   VARCHAR PRIMARY KEY,
    city          VARCHAR NOT NULL,
    joined_date   DATE    NOT NULL,
    tier          VARCHAR NOT NULL,
    phone_masked  VARCHAR NOT NULL
);
INSERT INTO dim_customer
SELECT customer_id, city, joined_date::DATE, tier,
       repeat('*', greatest(length(phone) - 3, 0)) || right(phone, 3)
FROM raw_customers;

-- Lịch 2025–2027, cờ cuối tuần + Tết (mùng 1 theo coffeegen.config.TET_MUNG_1) — giống dbt dim_date.
CREATE OR REPLACE TABLE dim_date (
    date_day        DATE PRIMARY KEY,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    iso_weekday     INTEGER NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_tet_season   BOOLEAN NOT NULL,   -- 7 ngày trước tới mùng 6
    is_tet_holiday  BOOLEAN NOT NULL    -- mùng 1–3
);
INSERT INTO dim_date
WITH days AS (
    SELECT d::DATE AS date_day FROM range(DATE '2025-01-01', DATE '2028-01-01', INTERVAL 1 DAY) t(d)
),
tet AS (
    SELECT * FROM (VALUES (DATE '2025-01-29'), (DATE '2026-02-17'), (DATE '2027-02-06')) t(mung1)
)
SELECT
    d.date_day,
    extract(year FROM d.date_day),
    extract(month FROM d.date_day),
    extract(isodow FROM d.date_day),
    extract(isodow FROM d.date_day) IN (6, 7),
    EXISTS (SELECT 1 FROM tet WHERE d.date_day BETWEEN tet.mung1 - 7 AND tet.mung1 + 5),
    EXISTS (SELECT 1 FROM tet WHERE d.date_day BETWEEN tet.mung1 AND tet.mung1 + 2)
FROM days d;

-- ═══════════════════════════════════════════════════════════════════════════════
-- 3. Fact — grain = 1 LINE ITEM (1 món trong 1 hoá đơn). Chọn grain nhỏ nhất có thể vì
--    "doanh thu theo món" không trả lời được ở grain hoá đơn. Mọi measure đều additive.
--    channel, payment_method, size: degenerate dimension — ít giá trị, không có thuộc tính → để thẳng trong fact.
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE TABLE fct_sales (
    line_id          VARCHAR PRIMARY KEY,
    transaction_id   VARCHAR NOT NULL,
    business_date    DATE    NOT NULL REFERENCES dim_date (date_day),
    ordered_at_utc   TIMESTAMPTZ NOT NULL,
    store_id         VARCHAR NOT NULL REFERENCES dim_store (store_id),
    customer_id      VARCHAR REFERENCES dim_customer (customer_id),   -- NULL = khách vãng lai
    item_id          VARCHAR NOT NULL REFERENCES dim_menu_item (item_id),
    channel          VARCHAR NOT NULL,
    payment_method   VARCHAR NOT NULL,
    size             VARCHAR NOT NULL,
    quantity         INTEGER NOT NULL,
    unit_price_vnd   INTEGER NOT NULL,
    line_total_vnd   INTEGER NOT NULL,
    line_cost_vnd    INTEGER NOT NULL,
    line_margin_vnd  INTEGER NOT NULL,
    is_rainy_day     BOOLEAN NOT NULL
);
INSERT INTO fct_sales
SELECT
    l.line_id,
    t.transaction_id,
    (t.ordered_at_utc AT TIME ZONE 'Asia/Ho_Chi_Minh')::DATE,
    t.ordered_at_utc,
    t.store_id,
    t.customer_id,
    l.item_id,
    t.channel,
    t.payment_method,
    l.size,
    l.quantity,
    l.unit_price_vnd,
    l.line_total_vnd,
    l.quantity * m.cost_vnd,
    l.line_total_vnd - l.quantity * m.cost_vnd,
    t.is_rainy_day
FROM lines_clean l
JOIN txn_clean t      USING (transaction_id)
JOIN dim_menu_item m  USING (item_id);

-- ═══════════════════════════════════════════════════════════════════════════════
-- 4. Câu hỏi nghiệp vụ: doanh thu / margin theo ngày × cửa hàng × món × kênh — 1 fact JOIN 3 dim, không CTE
-- ═══════════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW rpt_revenue_margin AS
SELECT
    f.business_date,
    d.is_tet_season,
    s.city,
    s.store_name,
    m.category,
    m.item_name,
    f.channel,
    sum(f.quantity)        AS qty,
    sum(f.line_total_vnd)  AS revenue_vnd,
    sum(f.line_margin_vnd) AS margin_vnd
FROM fct_sales f
JOIN dim_store s     USING (store_id)
JOIN dim_menu_item m USING (item_id)
JOIN dim_date d      ON d.date_day = f.business_date
GROUP BY ALL;
