{{ config(materialized='table') }}

with fhv_data as (
    select *, 
        'FHV' as service_type 
    from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    -- identifiers 
    fhv_data.tripid, 
    fhv_data.service_type, 
    fhv_data.dispatching_base_num, 
    coalesce(cast(sr_flag as integer),0) as sr_flag,
    fhv_data.affiliated_base_number,

    -- location information
    fhv_data.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,

    -- timestamp  
    fhv_data.pickup_datetime, 
    fhv_data.dropoff_datetime,

    -- time derived 
    EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
    EXTRACT(MONTH FROM pickup_datetime) AS pickup_month,
    EXTRACT(DAY FROM pickup_datetime) AS pickup_dom,
    EXTRACT(YEAR FROM pickup_datetime) AS pickup_year,
    EXTRACT(DAYOFWEEK FROM pickup_datetime) AS pickup_dow,
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM pickup_datetime) = 1 THEN 'Sun'
        WHEN EXTRACT(DAYOFWEEK FROM pickup_datetime) = 2 THEN 'Mon'
        WHEN EXTRACT(DAYOFWEEK FROM pickup_datetime) = 3 THEN 'Tue'
        WHEN EXTRACT(DAYOFWEEK FROM pickup_datetime) = 4 THEN 'Wed'
        WHEN EXTRACT(DAYOFWEEK FROM pickup_datetime) = 5 THEN 'Thu'
        WHEN EXTRACT(DAYOFWEEK FROM pickup_datetime) = 6 THEN 'Fri'
        WHEN EXTRACT(DAYOFWEEK FROM pickup_datetime) = 7 THEN 'Sat'
        ELSE NULL
    END AS pickup_dow_desc,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS trip_duration_min,
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM pickup_datetime) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS is_weekend_flag
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid