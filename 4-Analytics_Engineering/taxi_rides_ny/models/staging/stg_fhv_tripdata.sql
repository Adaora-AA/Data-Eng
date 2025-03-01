<<<<<<< HEAD:4-Analytics_Engineering/analytics_engineering/taxi_rides_ny/models/staging/stg_fhv_tripdata.sql
{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_taxi_data') }}
  where dispatching_base_num is not null 
)
select
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    sr_flag,
    affiliated_base_number
from tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
--{% if var('is_test_run', default=true) %}

  --limit 100

=======
{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *
  from {{ source('staging','fhv_taxi_data') }}
  where dispatching_base_num is not null 
)

select
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    sr_flag,
    affiliated_base_number
from tripdata

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
--{% if var('is_test_run', default=true) %}

  --limit 100

>>>>>>> ae16ab63db0d4470c28b8377ba0be0deab2ff0c6:4-Analytics_Engineering/taxi_rides_ny/models/staging/stg_fhv_tripdata.sql
--{% endif %}