{{
    config(
        materialized='table'
    )
}}

WITH trips_data AS (
    select * FROM {{ ref("fact_trips") }}
    ),
quarterly_revenue AS (
    SELECT service_type, year, quarter,year_quarter, sum(total_amount) AS curr_revenue
    FROM trips_data
    GROUP BY 1,2, 3,4
)
SELECT
    q1.*,
    q2.curr_revenue AS prev_rev,
    round(
        (q1.curr_revenue - q2.curr_revenue)
         /q2.curr_revenue * 100, 2
     ) AS yoy_rev_growth_percentage
FROM quarterly_revenue q1 
    LEFT JOIN quarterly_revenue q2 on q1.quarter = q2.quarter 
    AND q1.year = q2.year + 1
    AND q1.service_type = q2.service_type