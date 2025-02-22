{{ config(materialized="table") }}

with trips_data as (select * from {{ ref("fact_trips") }})

with
    quarterly_revenue as (
        select service_type, year, quarter, year_quarter, sum(total_amount) as revenue
        from trips_data
        group by 2, 3, 4
    )
select
    q1.service_type as service_type,
    q1.year as year,
    q1.quarter as quarter,
    q1.year_quarter as year_quarter,
    q1.revenue as curr_year_rev,
    q2.revenue as last_year_rev,
    round(
        (q1.curr_year_rev - q2.last_year_rev) / q2.last_year_rev * 100, 2
    ) as yoy_rev_growth_percentage
from quarterly_revenue q1
left join quarterly_revenue q2 on q1.quarter = q2.quarter and q1.year = q2.year + 1
