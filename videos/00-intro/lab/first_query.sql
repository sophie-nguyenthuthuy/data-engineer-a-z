-- Lab #00: câu query đầu tiên trên dữ liệu POS.
-- Chạy: docker compose exec -T postgres psql -U coffee -d coffee_pos < videos/00-intro/lab/first_query.sql

SELECT
    (ts AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS business_date,
    count(*)                                   AS transactions,
    -- TODO: thêm tổng doanh thu (cột total_amount_vnd), đặt tên revenue_vnd
    NULL::bigint                               AS revenue_vnd
FROM pos_transactions
GROUP BY 1
ORDER BY 1;
