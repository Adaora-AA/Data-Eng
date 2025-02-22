<<<<<<< HEAD:4-Analytics_Engineering/analytics_engineering/taxi_rides_ny/models/core/fct_fhv_monthly_zone_traveltime_p90.sql
WITH trip_durations AS (
    SELECT
        year,
        month,
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        dropoff_zone,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref("dim_fhv_trips") }}
)
SELECT
        year,
        month,
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        dropoff_zone,
    PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY year, month,pickup_locationid,dropoff_locationid) AS p90_trip_duration
=======
WITH trip_durations AS (
    SELECT
        year,
        month,
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        dropoff_zone,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref("dim_fhv_trips") }}
)
SELECT
        year,
        month,
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        dropoff_zone,
    PERCENTILE_CONT(trip_duration, 0.90) OVER (PARTITION BY year, month,pickup_locationid,pickup_locationid) AS p90_trip_duration
>>>>>>> ae16ab63db0d4470c28b8377ba0be0deab2ff0c6:4-Analytics_Engineering/taxi_rides_ny/models/core/fct_fhv_monthly_zone_traveltime_p90.sql
FROM trip_durations