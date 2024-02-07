{{ config(materialized='table') }}

select *
from {{ ref('fact_trips_full') }}

WHERE EXTRACT(YEAR FROM pickup_datetime) IN (2019, 2020) 
    AND total_amount < 2000
    AND total_amount > 0
    AND trip_duration_min < 600