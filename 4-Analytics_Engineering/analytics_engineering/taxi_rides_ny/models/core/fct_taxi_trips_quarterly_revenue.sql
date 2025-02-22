{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select * from {{ ref("fact_trips") }}
    ),
quarterly_revenue as (
    select service_type, year, quarter, year_quarter, sum(total_amount) as revenue
    from trips_data
    group by 1,2, 3, 4
)
select
    service_type,
    year,
    quarter,
    year_quarter,
    revenue as curr_year_rev,
    LAG(revenue) OVER(PARTITION BY quarter ORDER BY year) as last_year_rev,
    round(
        (revenue - LAG(revenue) OVER(PARTITION BY quarter ORDER BY year))
         / LAG(revenue) OVER(PARTITION BY quarter ORDER BY year) * 100, 2
    ) as yoy_rev_growth_percentage
from quarterly_revenue 