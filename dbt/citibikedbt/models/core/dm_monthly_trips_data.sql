{{ config(materialized='table') }}


with trips_data as (
    select * from {{ ref('fact_trips') }}
)

select 
date_trunc(starttime, month) as month,
avg(tripduration) as trip_duration_avg,
count(tripid) as total_monthly_trips,

from trips_data
group by month