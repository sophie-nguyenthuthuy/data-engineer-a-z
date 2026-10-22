-- Singular test: doanh thu ngày không bao giờ âm (nếu âm → silver lọt quantity âm).
select business_date, store_id, revenue_vnd
from {{ ref('fct_daily_store_revenue') }}
where revenue_vnd < 0
