{{ config(
    materialized='table'
) }}

select distinct
    -- La LocationID es la clave natural y subrogada
    cast(locationid as int) as zone_sk, 
    Borough,
    Zone,
    service_zone
from {{ source('bronze', 'taxi_zones') }}
where locationid is not null