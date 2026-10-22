with src as (
    select * from {{ source('silver', 'pos_line_items') }}
)
select
    line_id,
    transaction_id,
    item_id,
    size,
    quantity,
    unit_price_vnd,
    line_total_vnd
from src
