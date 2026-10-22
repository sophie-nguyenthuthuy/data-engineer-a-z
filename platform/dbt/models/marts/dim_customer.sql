select
    customer_id,
    city,
    joined_date,
    tier,
    phone_masked
from {{ ref('stg_customers') }}
