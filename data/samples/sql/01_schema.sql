-- Schema của database POS (OLTP) — chạy tự động khi Postgres khởi tạo volume lần đầu.
-- Bảng raw cố ý KHÔNG có primary key trên transaction_id: dữ liệu nguồn có bản ghi trùng,
-- và video #03/#07 dạy cách xử lý. Bảng dimension thì có PK bình thường.

DROP TABLE IF EXISTS pos_line_items;
DROP TABLE IF EXISTS pos_transactions;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS menu_items;
DROP TABLE IF EXISTS stores;

CREATE TABLE stores (
    store_id    TEXT PRIMARY KEY,
    store_name  TEXT NOT NULL,
    city        TEXT NOT NULL,
    district    TEXT NOT NULL,
    opened_date DATE NOT NULL,
    seats       INT  NOT NULL,
    tier        NUMERIC(3,2) NOT NULL
);

CREATE TABLE menu_items (
    item_id   TEXT PRIMARY KEY,
    item_name TEXT NOT NULL,
    category  TEXT NOT NULL,
    price_vnd INT  NOT NULL,
    cost_vnd  INT  NOT NULL
);

CREATE TABLE customers (
    customer_id TEXT PRIMARY KEY,
    full_name   TEXT NOT NULL,
    phone       TEXT NOT NULL,   -- PII: video #23 dạy masking
    email       TEXT NOT NULL,   -- PII
    city        TEXT NOT NULL,
    joined_date DATE NOT NULL,
    tier        TEXT NOT NULL
);

CREATE TABLE pos_transactions (
    transaction_id   TEXT        NOT NULL,          -- không UNIQUE: có duplicate cố ý
    store_id         TEXT        NULL,              -- có NULL cố ý
    ts               TIMESTAMPTZ NOT NULL,          -- một số dòng thiếu offset → bị hiểu là UTC (tz drift)
    channel          TEXT        NOT NULL,          -- store | app
    payment_method   TEXT        NOT NULL,
    customer_id      TEXT        NULL,
    staff_id         TEXT        NOT NULL,
    total_amount_vnd BIGINT      NOT NULL,
    discount_vnd     INT         NOT NULL DEFAULT 0,
    is_rainy_day     BOOLEAN     NOT NULL,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_pos_transactions_ts ON pos_transactions (ts);
CREATE INDEX idx_pos_transactions_store ON pos_transactions (store_id);

CREATE TABLE pos_line_items (
    line_id        TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    item_id        TEXT NOT NULL,
    size           TEXT NOT NULL,
    quantity       INT  NOT NULL,                   -- có số âm cố ý
    unit_price_vnd INT  NOT NULL,
    line_total_vnd INT  NOT NULL
);
CREATE INDEX idx_pos_line_items_txn ON pos_line_items (transaction_id);

-- Bảng không có PK → Debezium cần REPLICA IDENTITY FULL để thấy UPDATE/DELETE (video #16).
ALTER TABLE pos_transactions REPLICA IDENTITY FULL;
ALTER TABLE pos_line_items  REPLICA IDENTITY FULL;

-- Publication cho Debezium (video #16). Tạo sẵn để lab chỉ cần đăng ký connector.
DROP PUBLICATION IF EXISTS dbz_publication;
CREATE PUBLICATION dbz_publication FOR TABLE pos_transactions, pos_line_items, customers, menu_items, stores;
