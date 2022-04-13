{{ config(materialized='table') }}

select * from {{ ref('staging_citibike_tripdata') }}