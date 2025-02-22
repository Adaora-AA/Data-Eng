<<<<<<< HEAD:4-Analytics_Engineering/analytics_engineering/taxi_rides_ny/models/core/fct_taxi_trips_monthly_fare_p95.sql
{{
    config(
        materialized='table'
    )
}}

WITH trips_data AS (
    SELECT * FROM {{ ref("fact_trips") }}
    ),
valid_trips AS(
    SELECT
    service_type,
    year,
    month,
    fare_amount
    FROM trips_data
    WHERE fare_amount > 0 
        AND trip_distance > 0 
        AND payment_type_description IN ('Cash', 'Credit card')
)
SELECT
    service_type,
    year,
    month,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97_fare,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95_fare,
    PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90_fare
=======

{{
    config(
        materialized='table'
    )
}}

WITH trips_data AS (
    SELECT * FROM {{ ref("fact_trips") }}
    ),
valid_trips AS(
    SELECT
    service_type,
    year,
    month,
    fare_amount
    FROM trips_data
    WHERE fare_amount > 0 
        AND trip_distance > 0 
        AND payment_type_description IN ('Cash', 'Credit Card')
)
SELECT
    service_type,
    year,
    month,
    PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97_fare,
    PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95_fare,
    PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90_fare
>>>>>>> ae16ab63db0d4470c28b8377ba0be0deab2ff0c6:4-Analytics_Engineering/taxi_rides_ny/models/core/fct_taxi_trips_monthly_fare_p95.sql
FROM valid_trips