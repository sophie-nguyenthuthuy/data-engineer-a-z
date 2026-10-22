-- Nạp dữ liệu mẫu từ CSV đã commit trong data/samples (mount tại /docker-entrypoint-initdb.d/..).
-- Server-side COPY: Postgres tự đọc file trong container.
\set ON_ERROR_STOP on

COPY stores      FROM '/opt/samples/stores.csv'       WITH (FORMAT csv, HEADER true);
COPY menu_items  FROM '/opt/samples/menu_items.csv'   WITH (FORMAT csv, HEADER true);
COPY customers   FROM '/opt/samples/customers.csv'    WITH (FORMAT csv, HEADER true);
COPY pos_transactions (transaction_id, store_id, ts, channel, payment_method, customer_id, staff_id, total_amount_vnd, discount_vnd, is_rainy_day)
                 FROM '/opt/samples/pos_transactions.csv' WITH (FORMAT csv, HEADER true, NULL '');
COPY pos_line_items FROM '/opt/samples/pos_line_items.csv' WITH (FORMAT csv, HEADER true);

ANALYZE;
SELECT 'stores' AS t, count(*) FROM stores
UNION ALL SELECT 'menu_items', count(*) FROM menu_items
UNION ALL SELECT 'customers', count(*) FROM customers
UNION ALL SELECT 'pos_transactions', count(*) FROM pos_transactions
UNION ALL SELECT 'pos_line_items', count(*) FROM pos_line_items;
