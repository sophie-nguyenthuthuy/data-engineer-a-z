-- Lịch 2025–2027 với cờ cuối tuần và Tết (mùng 1 hard-code theo coffeegen.config.TET_MUNG_1).
with days as (
    select cast(d as date) as date_day
    from range(cast('2025-01-01' as date), cast('2028-01-01' as date), interval 1 day) t(d)
),
tet as (
    select * from (values (cast('2025-01-29' as date)), (cast('2026-02-17' as date)), (cast('2027-02-06' as date))) t(mung1)
)
select
    d.date_day,
    extract(year from d.date_day)                       as year,
    extract(month from d.date_day)                      as month,
    extract(isodow from d.date_day)                     as iso_weekday,
    extract(isodow from d.date_day) in (6, 7)           as is_weekend,
    exists (select 1 from tet where d.date_day between tet.mung1 - 7 and tet.mung1 + 5) as is_tet_season,
    exists (select 1 from tet where d.date_day between tet.mung1 and tet.mung1 + 2)     as is_tet_holiday
from days d
