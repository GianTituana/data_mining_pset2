{{ config(
    materialized='table',
    cluster_by=['pickup_date_sk', 'pu_zone_sk'] 
) }}

with trips as (
    select *
    from {{ ref('stg_enriched') }} -- Consume el modelo final de Silver
),

final_fact as (
    select
        -- CLAVE PRIMARIA DEL HECHO (Hash del registro)
        {{ dbt_utils.generate_surrogate_key([
            'pickup_datetime', 'dropoff_datetime', 'pu_location_id', 
            'do_location_id', 'total_amount', 'service_type'
        ]) }} as trip_id, 

        -- CLAVES SUBROGADAS (SK)
        -- Claves de Fecha (Unir con dim_date)
        D_PICKUP.date_sk as pickup_date_sk,
        D_DROPOFF.date_sk as dropoff_date_sk,
        
        -- Claves de Zona (LocationID)
        T1.pu_location_id as pu_zone_sk, 
        T1.do_location_id as do_zone_sk,
        
        -- Otras Claves de Dimensión
        PT.payment_type_sk,
        V.vendor_sk,
        RC.rate_code_sk,
        ST.service_type_sk,

        -- MÉTRICAS (Medidas)
        T1.trip_distance,
        T1.trip_duration_seconds,
        T1.fare_amount,
        T1.total_amount,
        T1.tip_amount,
        T1.extra,
        T1.mta_tax,
        T1.tolls_amount,
        T1.congestion_surcharge,
        T1.airport_fee,
        T1.cbd_congestion_fee,

        -- Metadatos para auditoría
        T1.service_type
        
    from trips T1
    
    -- UNIONES A DIMENSIONES
    left join {{ ref('dim_date') }} D_PICKUP 
        on date_trunc('day', T1.pickup_datetime)::date = D_PICKUP.full_date
    left join {{ ref('dim_date') }} D_DROPOFF 
        on date_trunc('day', T1.dropoff_datetime)::date = D_DROPOFF.full_date
        
    left join {{ ref('dim_payment_type') }} PT on T1.payment_type = PT.payment_type
    left join {{ ref('dim_vendor') }} V on T1.vendor_id = V.vendor_sk
    left join {{ ref('dim_rate_code') }} RC on T1.rate_code_id = RC.rate_code_sk
    left join {{ ref('dim_service_type') }} ST on T1.service_type = ST.service_type_name
)

select * from final_fact