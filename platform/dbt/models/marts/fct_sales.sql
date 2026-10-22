-- Fact ở grain line item: 1 dòng = 1 món trong 1 hoá đơn. Star schema: join tới dim_store, dim_menu_item, dim_customer, dim_date.
{{ config(materialized='table') }}

select
    l.line_id,
    t.transaction_id,
    t.business_date,
    t.ordered_at_utc,
    t.store_id,
    t.customer_id,
    l.item_id,
    t.channel,
    t.payment_method,
    l.size,
    l.quantity,
    l.unit_price_vnd,
    l.line_total_vnd,
    l.quantity * m.cost_vnd                        as line_cost_vnd,
    l.line_total_vnd - l.quantity * m.cost_vnd     as line_margin_vnd,
    t.is_rainy_day
from {{ ref('stg_pos_line_items') }} l
join {{ ref('stg_pos_transactions') }} t using (transaction_id)
left join {{ ref('stg_menu_items') }} m using (item_id)
