{{ config(materialized='table') }}

with green_data as (
    select *, 
        'Green' as service_type 
    from {{ ref('stg_green_tripdata') }}
), 

yellow_data as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 

trips_unioned as (
    select * from green_data
    union all
    select * from yellow_data
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    -- identifiers
    trips_unioned.tripid, 
    trips_unioned.vendorid, 
    trips_unioned.service_type,
    trips_unioned.ratecodeid, 

    -- location  
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,

    -- timestamp   
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 

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
    END AS is_weekend_flag,

    -- trip info 
    trips_unioned.store_and_fwd_flag,
    trips_unioned.passenger_count, 
    trips_unioned.trip_distance, 
    trips_unioned.trip_type, 

    -- payment info 
    trips_unioned.fare_amount, 
    trips_unioned.extra, 
    trips_unioned.mta_tax, 
    trips_unioned.tip_amount, 
    trips_unioned.tolls_amount, 
    trips_unioned.ehail_fee, 
    trips_unioned.improvement_surcharge, 
    trips_unioned.total_amount, 
    trips_unioned.payment_type, 
    trips_unioned.payment_type_description,

    -- payment info derived 
    trips_unioned.extra + trips_unioned.mta_tax + trips_unioned.tolls_amount + trips_unioned.ehail_fee + trips_unioned.improvement_surcharge AS total_fees_and_tax
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid
