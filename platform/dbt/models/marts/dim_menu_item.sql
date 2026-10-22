select
    item_id,
    item_name,
    category,
    price_vnd,
    cost_vnd,
    price_vnd - cost_vnd                                       as margin_vnd,
    round((price_vnd - cost_vnd) * 100.0 / price_vnd, 1)       as margin_pct
from {{ ref('stg_menu_items') }}
