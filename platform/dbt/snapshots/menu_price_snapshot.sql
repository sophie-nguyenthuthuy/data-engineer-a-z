{% snapshot menu_price_snapshot %}
{{
    config(
      target_schema='snapshots',
      unique_key='item_id',
      strategy='check',
      check_cols=['price_vnd', 'cost_vnd'],
    )
}}
-- SCD type 2 cho bảng giá: mỗi lần giá đổi, dbt đóng dòng cũ (dbt_valid_to) và mở dòng mới.
select item_id, item_name, category, price_vnd, cost_vnd
from {{ source('silver', 'menu_items') }}
qualify row_number() over (partition by item_id order by dt desc) = 1
{% endsnapshot %}
