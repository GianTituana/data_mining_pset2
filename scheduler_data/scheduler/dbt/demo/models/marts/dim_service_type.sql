{{ config(
    materialized='table'
) }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['service_type']) }} as service_type_sk,
    service_type as service_type_name
from {{ ref('stg_enriched') }}
where service_type is not null