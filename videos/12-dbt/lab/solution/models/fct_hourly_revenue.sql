-- Lời giải lab #12: doanh thu theo GIỜ VIỆT NAM × cửa hàng. Grain = (business_date, store_id, hour_local).
-- Nguồn: staging đã đổi UTC → giờ VN (ordered_at_local), nên ở đây chỉ cần extract(hour).
{{ config(materialized='table') }}

select
    t.business_date,
    t.store_id,
    extract(hour from t.ordered_at_local)                                   as hour_local,
    count(distinct t.transaction_id)                                        as transactions,
    sum(t.total_amount_vnd)                                                 as revenue_vnd,
    sum(case when t.channel = 'app' then t.total_amount_vnd else 0 end)     as app_revenue_vnd,
    round(sum(t.total_amount_vnd) * 1.0 / count(distinct t.transaction_id)) as avg_ticket_vnd
from {{ ref('stg_pos_transactions') }} t
group by 1, 2, 3
