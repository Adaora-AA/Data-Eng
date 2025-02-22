<<<<<<< HEAD:4-Analytics_Engineering/analytics_engineering/taxi_rides_ny/models/core/fct_taxi_trips_quarterly_revenue.sql
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
=======
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
>>>>>>> ae16ab63db0d4470c28b8377ba0be0deab2ff0c6:4-Analytics_Engineering/taxi_rides_ny/models/core/fct_taxi_trips_quarterly_revenue.sql
