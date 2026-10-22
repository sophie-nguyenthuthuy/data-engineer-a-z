-- Bảng tổng hợp ngày × cửa hàng — nguồn cho dashboard doanh thu.
select
    t.business_date,
    t.store_id,
    count(distinct t.transaction_id)                                   as transactions,
    sum(t.total_amount_vnd)                                            as revenue_vnd,
    sum(t.discount_vnd)                                                as discount_vnd,
    sum(case when t.channel = 'app' then t.total_amount_vnd else 0 end) as app_revenue_vnd,
    round(sum(t.total_amount_vnd) * 1.0 / count(distinct t.transaction_id)) as avg_ticket_vnd,
    bool_or(t.is_rainy_day)                                            as is_rainy_day
from {{ ref('stg_pos_transactions') }} t
group by 1, 2
