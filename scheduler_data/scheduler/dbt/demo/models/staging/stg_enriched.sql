{{ config(
    materialized='table'
) }}

with trips as (
    select *
    from {{ ref('stg_trips') }} 
),
zones as (
    select *
    from {{ source('bronze', 'taxi_zones') }}
)

select
    t.*,
    DATEDIFF(second, t.pickup_datetime, t.dropoff_datetime) AS trip_duration_seconds,
    zp.zone as pickup_zone,
    zp.borough as pickup_borough,
    zd.zone as dropoff_zone,
    zd.borough as dropoff_borough
from trips t
left join zones zp on t.pu_location_id = zp.zone
left join zones zd on t.do_location_id = zd.borough