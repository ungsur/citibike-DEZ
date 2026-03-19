{{ config(materialized='table') }}


with trips_data as (
    select * from {{ ref('fact_trips') }}
)

select 
format_datetime('%A', started_at) as day_of_week,
avg(tripduration) as trip_duration_avg_by_day,
count(ride_id) as total_daily_trips,

from trips_data
group by day_of_week