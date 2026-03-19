{{ config(materialized='table') }}


with trips_data as (
    select ride_id,
    start_station_id, start_station_name, start_lat, start_lng,
     from {{ ref('fact_trips') }}
)

select 
count(ride_id) as total_trips_from_station,
start_station_name,start_lat,
    start_lng
from trips_data
group by start_station_name,start_lat,
    start_lng