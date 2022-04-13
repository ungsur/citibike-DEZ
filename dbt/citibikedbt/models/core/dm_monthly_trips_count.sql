{{ config(materialized='table') }}


with trips_data as (
    select * from {{ ref('fact_trips') }}
)

select 
count(tripid) as tripcount,
date_trunc(starttime, month) as month,


from trips_data
group by month