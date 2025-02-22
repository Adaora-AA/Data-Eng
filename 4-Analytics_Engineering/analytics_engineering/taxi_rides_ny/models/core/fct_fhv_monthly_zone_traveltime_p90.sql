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
FROM trip_durations