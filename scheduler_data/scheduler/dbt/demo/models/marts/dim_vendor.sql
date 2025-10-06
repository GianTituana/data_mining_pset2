{{ config(
    materialized='table'
) }}


select distinct
    cast(vendor_id as int) as vendor_sk, 
    case vendor_id
        when 1 then 'Creative Mobile Technologies, LLC' 
        when 2 then 'Curb Mobility, LLC' 
        when 6 then 'Myle Technologies Inc' 
        when 7 then 'Helix' 
        else 'Unknown'
    end as vendor_name
from {{ ref('stg_enriched') }}
where vendor_id is not null