-- Iceberg DDL cho silver (video #17). Catalog `lake` = JDBC catalog trong Postgres (db iceberg_catalog),
-- cấu hình sẵn trong infra/docker/spark-defaults.conf; Trino dùng cùng catalog (infra/docker/trino/catalog/lake.properties).
-- Chạy: docker compose exec spark-master spark-sql -f /opt/lake/iceberg/silver_tables.sql
--   (hoặc copy từng lệnh vào notebook / Trino với catalog lake)

CREATE NAMESPACE IF NOT EXISTS lake.silver;

CREATE TABLE IF NOT EXISTS lake.silver.pos_transactions (
    transaction_id   STRING    NOT NULL,
    store_id         STRING    NOT NULL,
    ts               TIMESTAMP NOT NULL,     -- UTC
    channel          STRING,
    payment_method   STRING,
    customer_id      STRING,
    staff_id         STRING,
    total_amount_vnd BIGINT,
    discount_vnd     BIGINT,
    is_rainy_day     BOOLEAN,
    _tz_fixed        BOOLEAN
)
USING iceberg
PARTITIONED BY (days(ts))
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.target-file-size-bytes' = '134217728'
);

CREATE TABLE IF NOT EXISTS lake.silver.pos_line_items (
    line_id        STRING NOT NULL,
    transaction_id STRING NOT NULL,
    item_id        STRING,
    size           STRING,
    quantity       INT,
    unit_price_vnd BIGINT,
    line_total_vnd BIGINT
)
USING iceberg
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS lake.silver.iot_readings (
    reading_id        STRING,
    machine_id        STRING,
    store_id          STRING,
    ts                TIMESTAMP,
    ingested_at       TIMESTAMP,
    boiler_temp_c     DOUBLE,
    pump_pressure_bar DOUBLE,
    water_flow_ml_s   DOUBLE,
    shots_total       BIGINT,
    status            STRING,
    firmware          STRING
)
USING iceberg
PARTITIONED BY (hours(ts), bucket(8, machine_id))
TBLPROPERTIES ('format-version' = '2');

-- Thử time travel sau khi có vài snapshot:
--   SELECT * FROM lake.silver.pos_transactions.snapshots;
--   SELECT count(*) FROM lake.silver.pos_transactions VERSION AS OF <snapshot_id>;
