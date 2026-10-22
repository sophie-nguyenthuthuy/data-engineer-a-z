-- Lab #12: doanh thu theo GIỜ VIỆT NAM × cửa hàng. Grain = (business_date, store_id, hour_local).
-- Copy file này + fct_hourly_revenue.yml vào platform/dbt/models/marts/ rồi:
--   cd platform/dbt && uv run dbt build --profiles-dir . --target dev --select fct_hourly_revenue
{{ config(materialized='table') }}

select
    t.business_date,
    t.store_id,
    -- TODO 1: cột hour_local = giờ địa phương. stg_pos_transactions đã có ordered_at_local (giờ VN, không tz)
    --   gợi ý: extract(hour from t.ordered_at_local)
    0                                      as hour_local,
    -- TODO 2: transactions = số hoá đơn distinct; revenue_vnd = tổng total_amount_vnd
    0                                      as transactions,
    0                                      as revenue_vnd,
    -- TODO 3: app_revenue_vnd = doanh thu riêng kênh app (xem cách fct_daily_store_revenue làm)
    0                                      as app_revenue_vnd
from {{ ref('stg_pos_transactions') }} t
-- TODO 4: group by đúng grain
group by 1, 2
