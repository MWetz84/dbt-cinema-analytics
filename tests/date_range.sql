-- Select invalid rows - 
with
    month_select as (
        select date_trunc('month', timestamp) as monat
        from {{ source("silverscreen", "nj_001") }}
        union all
        select date_trunc('month', date)
        from {{ source("silverscreen", "nj_002") }}
        union all
        select date_trunc('month', timestamp)
        from {{ source("silverscreen", "nj_003") }}
    ),

    month_range as (
        select min(monat) as min_month, max(monat) as max_month from month_select
    )

select *
from {{ ref("mart_movies_efficiency") }}
where
    month > (select max_month from month_range)
    or month < (select min_month from month_range)
