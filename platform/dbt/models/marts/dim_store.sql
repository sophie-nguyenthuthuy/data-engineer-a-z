select
    store_id,
    store_name,
    city,
    district,
    opened_date,
    seats,
    tier,
    case when tier >= 1.2 then 'flagship' when tier >= 0.9 then 'standard' else 'kiosk' end as store_class
from {{ ref('stg_stores') }}
