<<<<<<< HEAD:4-Analytics_Engineering/analytics_engineering/taxi_rides_ny/models/core/dim_fhv_trips.sql
{{ config(materialized="table") }}

with
    fhv_tripdata as (
        select * from {{ ref("stg_fhv_tripdata") }}
        ),
    dim_zones as (
        select * from {{ ref("dim_zones") }}
            where borough != 'Unknown'
            )
select
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.sr_flag,
    fhv_tripdata.affiliated_base_number,
    extract(year from pickup_datetime) as year,
    extract(month from pickup_datetime) as month
from fhv_tripdata
inner join
    dim_zones as pickup_zone 
        on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
        on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
=======
{{ config(materialized="table") }}

with
    fhv_tripdata as (select * from {{ ref("stg_fhv_tripdata") }}),
    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.pickup_locationid,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    fhv_tripdata.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime,
    fhv_tripdata.sr_flag,
    fhv_tripdata.affiliated_base_number,
    extract(year from pickup_datetime) as year,
    extract(quarter from pickup_datetime) as month
from fhv_tripdata
inner join
    dim_zones as pickup_zone on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
>>>>>>> ae16ab63db0d4470c28b8377ba0be0deab2ff0c6:4-Analytics_Engineering/taxi_rides_ny/models/core/dim_fhv_trips.sql
