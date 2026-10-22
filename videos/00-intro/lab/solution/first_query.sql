SELECT
    (ts AT TIME ZONE 'Asia/Ho_Chi_Minh')::date AS business_date,
    count(*)                                   AS transactions,
    sum(total_amount_vnd)                      AS revenue_vnd
FROM pos_transactions
GROUP BY 1
ORDER BY 1;

-- Bài tập 1: doanh thu theo thành phố
SELECT s.city, sum(t.total_amount_vnd) AS revenue_vnd
FROM pos_transactions t
JOIN stores s USING (store_id)
GROUP BY 1;

-- Bài tập 2: hoá đơn thiếu store_id (lỗi cố ý "null_store" — máy POS mới chưa cấu hình)
SELECT count(*) FROM pos_transactions WHERE store_id IS NULL;
