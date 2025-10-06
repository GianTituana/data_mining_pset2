{{ config(
    materialized='table'
) }}

select distinct
    cast(rate_code_id as int) as rate_code_sk, 
    case rate_code_id
        when 1 then 'Standard rate'
        when 2 then 'JFK'
        when 3 then 'Newark'
        when 4 then 'Nassau or Westchester'
        when 5 then 'Negotiated fare'
        when 6 then 'Group ride'
        when 99 then 'Null/unknown'
        else 'Other'
    end as rate_code_name
from {{ ref('stg_enriched') }}
where rate_code_id is not null
