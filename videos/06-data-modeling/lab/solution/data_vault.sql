-- Lời giải #06 (phần Data Vault) — một ví dụ hub / link / satellite cho store và machine.
-- Chạy: uv run python videos/06-data-modeling/lab/load_star.py videos/06-data-modeling/lab/solution/data_vault.sql
--
-- Data Vault tách 3 thứ mà star schema trộn chung trong một dim:
--   hub       = "thực thể này tồn tại" (chỉ business key)
--   link      = "hai thực thể liên quan" (chỉ khoá)
--   satellite = "thực thể trông như thế nào tại thời điểm load" (thuộc tính + hash_diff → SCD2 tự nhiên)
-- Mọi bảng đều có load_dts (lúc nạp) và record_source (nạp từ đâu) — Vault sinh ra cho audit.

CREATE OR REPLACE TABLE raw_stores   AS SELECT * FROM read_csv('data/samples/stores.csv');
CREATE OR REPLACE TABLE raw_machines AS SELECT * FROM read_csv('data/samples/machines.csv');

-- ── HUB: chỉ business key. hash key = md5(business key) để mọi hệ thống nguồn tạo ra cùng khoá độc lập nhau.
CREATE OR REPLACE TABLE hub_store (
    store_hk       VARCHAR PRIMARY KEY,      -- md5(store_id)
    store_id       VARCHAR NOT NULL UNIQUE,  -- business key
    load_dts       TIMESTAMPTZ NOT NULL,
    record_source  VARCHAR NOT NULL
);
INSERT INTO hub_store
SELECT md5(store_id), store_id, now(), 'pos.stores' FROM raw_stores;

CREATE OR REPLACE TABLE hub_machine (
    machine_hk     VARCHAR PRIMARY KEY,
    machine_id     VARCHAR NOT NULL UNIQUE,
    load_dts       TIMESTAMPTZ NOT NULL,
    record_source  VARCHAR NOT NULL
);
INSERT INTO hub_machine
SELECT md5(machine_id), machine_id, now(), 'iot.machines' FROM raw_machines;

-- ── LINK: quan hệ máy ↔ cửa hàng. Nếu máy được chuyển sang cửa hàng khác, thêm 1 dòng link mới (không UPDATE).
CREATE OR REPLACE TABLE link_store_machine (
    store_machine_hk  VARCHAR PRIMARY KEY,   -- md5(store_id || '|' || machine_id)
    store_hk          VARCHAR NOT NULL REFERENCES hub_store (store_hk),
    machine_hk        VARCHAR NOT NULL REFERENCES hub_machine (machine_hk),
    load_dts          TIMESTAMPTZ NOT NULL,
    record_source     VARCHAR NOT NULL
);
INSERT INTO link_store_machine
SELECT md5(store_id || '|' || machine_id), md5(store_id), md5(machine_id), now(), 'iot.machines'
FROM raw_machines;

-- ── SATELLITE: thuộc tính mô tả. PK = (hub key, load_dts). hash_diff = md5 của mọi thuộc tính:
--    lần load sau, nếu hash_diff đổi → INSERT dòng mới; không đổi → bỏ qua. Đó chính là SCD2 không cần valid_to.
CREATE OR REPLACE TABLE sat_store (
    store_hk       VARCHAR NOT NULL REFERENCES hub_store (store_hk),
    load_dts       TIMESTAMPTZ NOT NULL,
    hash_diff      VARCHAR NOT NULL,
    store_name     VARCHAR,
    city           VARCHAR,
    district       VARCHAR,
    opened_date    DATE,
    seats          INTEGER,
    tier           DECIMAL(3, 2),
    record_source  VARCHAR NOT NULL,
    PRIMARY KEY (store_hk, load_dts)
);
INSERT INTO sat_store
SELECT md5(store_id), now(),
       md5(concat_ws('|', store_name, city, district, opened_date, seats, tier)),
       store_name, city, district, opened_date, seats, tier, 'pos.stores'
FROM raw_stores;

CREATE OR REPLACE TABLE sat_machine (
    machine_hk      VARCHAR NOT NULL REFERENCES hub_machine (machine_hk),
    load_dts        TIMESTAMPTZ NOT NULL,
    hash_diff       VARCHAR NOT NULL,
    model           VARCHAR,
    installed_date  DATE,
    record_source   VARCHAR NOT NULL,
    PRIMARY KEY (machine_hk, load_dts)
);
INSERT INTO sat_machine
SELECT md5(machine_id), now(), md5(concat_ws('|', model, installed_date)), model, installed_date, 'iot.machines'
FROM raw_machines;

-- ── Dựng lại "dim_store hiện tại" từ Vault: hub + bản sat mới nhất. Star schema sống PHÍA TRÊN Vault.
CREATE OR REPLACE VIEW dim_store_from_vault AS
SELECT h.store_id, s.store_name, s.city, s.district, s.opened_date, s.seats, s.tier
FROM hub_store h
JOIN sat_store s USING (store_hk)
QUALIFY row_number() OVER (PARTITION BY h.store_hk ORDER BY s.load_dts DESC) = 1;

-- ── Câu hỏi Vault trả lời mà star không: "máy nào ở cửa hàng nào, theo nguồn nào, nạp lúc nào"
CREATE OR REPLACE VIEW rpt_machines_per_store AS
SELECT h.store_id, count(*) AS machines, min(l.load_dts) AS first_seen, l.record_source
FROM link_store_machine l
JOIN hub_store h USING (store_hk)
GROUP BY ALL;
