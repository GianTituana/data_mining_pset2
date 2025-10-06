{{ config(
    materialized='table'
) }}

with yellow as (
    select *, 'yellow' as service_type
    from {{ ref('stg_yellow') }}
),
green as (
    select *, 'green' as service_type
    from {{ ref('stg_green') }}
)

select *
from yellow
union all
select *
from green