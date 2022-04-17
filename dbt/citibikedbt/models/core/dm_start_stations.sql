{{ config(materialized='table') }}


with trips_data as (
    select tripid,
    start_station_id, start_station_name, start_station_latitude,
    start_station_longitude
     from {{ ref('fact_trips') }}
)

select 
count(tripid) as total_trips_from_station,
start_station_name,start_station_latitude,
    start_station_longitude

from trips_data
group by start_station_name,start_station_latitude,
    start_station_longitude