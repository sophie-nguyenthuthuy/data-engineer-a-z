-- Dimension được ingest lại NGUYÊN BẢN mỗi ngày (full snapshot) → mỗi partition dt= có 1 bản.
-- Staging chỉ lấy bản mới nhất. (Partition column `dt` có nhờ hive_partitioning = true.)
select
    store_id,
    store_name,
    city,
    district,
    opened_date,
    seats,
    tier
from {{ source('silver', 'stores') }}
qualify row_number() over (partition by store_id order by dt desc) = 1
