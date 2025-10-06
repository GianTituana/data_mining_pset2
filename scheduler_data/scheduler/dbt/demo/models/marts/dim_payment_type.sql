{{ config(
    materialized='table'
) }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['payment_type']) }} as payment_type_sk,
    payment_type
from {{ ref('stg_enriched') }}
where payment_type is not null