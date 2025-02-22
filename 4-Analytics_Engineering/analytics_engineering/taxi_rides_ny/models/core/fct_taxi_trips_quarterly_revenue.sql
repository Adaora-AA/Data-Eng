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
    service_type,
    year,
    quarter,
    year_quarter,
    revenue AS curr_year_rev,
    LAG(revenue) OVER(PARTITION BY quarter ORDER BY year) as last_year_rev,
    round(
        (revenue - LAG(revenue) OVER(PARTITION BY quarter ORDER BY year))
         / LAG(revenue) OVER(PARTITION BY quarter ORDER BY year) * 100, 2
    ) AS yoy_rev_growth_percentage
FROM quarterly_revenue 