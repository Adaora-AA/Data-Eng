{{
    config(
        materialized='table'
    )
}}

WITH trips_data AS (
    select * FROM {{ ref("fact_trips") }}
    ),
quarterly_revenue AS (
    SELECT service_type, year, quarter, year_quarter, sum(total_amount) AS revenue
    FROM trips_data
    GROUP BY 1,2, 3, 4
)
SELECT
    q1.service_type,
    q1.year,
    q1.quarter,
    q1.year_quarter,
    q1.revenue AS curr_year_rev,
    q2.revenue AS last_year_rev,
    round(
        (q1.revenue - q2.revenue)
         / q2.revenue * 100, 2
     ) AS yoy_rev_growth_percentage
FROM quarterly_revenue q1 join quarterly_revenue q2 on q1.quarter = q2.quarter and q1.year = q2.year + 1