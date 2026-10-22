-- Analysis (không materialize): doanh thu tuần Tết so với tuần thường. Chạy: dbt compile → copy SQL vào DuckDB.
select
    d.is_tet_holiday,
    sum(r.revenue_vnd) / count(distinct r.business_date) as avg_daily_revenue_vnd
from {{ ref('fct_daily_store_revenue') }} r
join {{ ref('dim_date') }} d on d.date_day = r.business_date
group by 1
