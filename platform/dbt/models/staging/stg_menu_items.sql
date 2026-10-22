select
    item_id,
    item_name,
    category,
    price_vnd,
    cost_vnd
from {{ source('silver', 'menu_items') }}
qualify row_number() over (partition by item_id order by dt desc) = 1
