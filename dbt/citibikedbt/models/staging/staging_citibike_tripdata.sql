{{ config(materialized='view') }}

select 
    -- identifiers
    cast(bikeid as integer) as bikeid,
    cast(start_station_id as integer) as start_station_id,
    cast(end_station_id as integer) as end_station_id,
    -- timestamps
    cast(starttime as timestamp) as starttime,
    cast(stoptime as timestamp) as stoptime,
    -- trip info
    cast(tripduration as integer) as tripduration,
    cast(start_station_name as string) as start_station_name,
    cast(start_station_latitude as float64) as start_station_latitude,
    cast(start_station_longitude as float64) as start_station_longitude,
    cast(end_station_name as string) as end_station_name,
    cast(end_station_latitude as float64) as end_station_latitude,
    cast(end_station_longitude as float64) as end_station_longitude,
    -- user info
    cast(usertype as string) as usertype,
    cast(birth_year as integer) as birth_year,
    cast(gender as integer) as gender,
    {{ get_gender_type_description('gender') }} as gender_type_desc

from {{ source('staging', 'citibike_table_partitioned') }}
limit 100


 
   