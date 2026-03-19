{{ config(materialized='view') }}

select 
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['ride_id','started_at'])}} as trip_id,
    cast(ride_id as string) as ride_id,
    cast(start_station_id as string) as start_station_id,
    cast(end_station_id as string) as end_station_id,
    -- timestamps
    cast(started_at as timestamp) as started_at,
    cast(ended_at as timestamp) as ended_at,
    -- trip info
    cast(started_at - ended_at as interval) as tripduration,
    cast(start_station_name as string) as start_station_name,
    cast(start_lat as float64) as start_lat,
    cast(start_lng as float64) as start_lng,
    cast(end_station_name as string) as end_station_name,
    cast(end_lat as float64) as end_lat,
    cast(end_lng as float64) as end_lng,
    -- user info
    cast(member_casual as string) as member_casual,
    cast(rideable_type as string) as rideable_type
from {{ source('staging', 'citibike_table_partitioned') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}