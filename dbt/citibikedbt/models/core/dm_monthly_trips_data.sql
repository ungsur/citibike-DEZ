{{ config(materialized='table') }}


with trips_data as (
    select * from {{ ref('fact_trips') }}
)

select 
date_trunc(started_at, month) as month,
avg(tripduration) as trip_duration_avg,
count(ride_id) as total_monthly_trips,
from trips_data
group by month