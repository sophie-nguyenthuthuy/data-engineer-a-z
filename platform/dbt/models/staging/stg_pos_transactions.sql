-- Staging: đổi tên cột về chuẩn, đổi UTC → giờ Việt Nam cho cột business_date.
with src as (
    select * from {{ source('silver', 'pos_transactions') }}
)
select
    transaction_id,
    store_id,
    customer_id,
    staff_id,
    channel,
    payment_method,
    ts                                                   as ordered_at_utc,
    ts at time zone 'Asia/Ho_Chi_Minh'                   as ordered_at_local,
    cast(ts at time zone 'Asia/Ho_Chi_Minh' as date)     as business_date,
    total_amount_vnd,
    discount_vnd,
    total_amount_vnd + discount_vnd                      as gross_amount_vnd,
    is_rainy_day
from src
