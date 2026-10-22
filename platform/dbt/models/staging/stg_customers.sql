-- PII (full_name, phone, email) KHÔNG đi tiếp lên gold. Video #23 giải thích tại sao và cách hash khi thật sự cần.
select
    customer_id,
    city,
    joined_date,
    tier,
    {{ mask_pii('phone') }} as phone_masked
from {{ source('silver', 'customers') }}
qualify row_number() over (partition by customer_id order by dt desc) = 1
