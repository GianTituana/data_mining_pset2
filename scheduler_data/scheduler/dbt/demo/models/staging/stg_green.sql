-- Modelo Silver: Yellow Taxi Trip, limpieza y estandarización
{{ config(materialized='table') }}

with raw as (
    select *
    from {{ source('bronze', 'green_tripdata') }}
),

standardized as (
    select
        -- Convertir fechas a timestamp y estandarizar zona horaria NYC
        convert_timezone('America/New_York', LPEP_PICKUP_DATETIME) as pickup_datetime,
        convert_timezone('America/New_York', LPEP_DROPOFF_DATETIME) as dropoff_datetime,
        -- Convertir distancias y tarifas a float
        cast(TRIP_DISTANCE as float) as trip_distance,
        cast(FARE_AMOUNT as float) as fare_amount,
        cast(TOTAL_AMOUNT as float) as total_amount,
        cast(TIP_AMOUNT as float) as tip_amount,
        cast(EXTRA as float) as extra,
        cast(MTA_TAX as float) as mta_tax,
        cast(TOLLS_AMOUNT as float) as tolls_amount,
        cast(IMPROVEMENT_SURCHARGE as float) as improvement_surcharge,
        cast(CONGESTION_SURCHARGE as float) as congestion_surcharge,
        null as airport_fee,
        cast(CBD_CONGESTION_FEE as float) as cbd_congestion_fee,  -- Nueva columna 2025
        -- Normalizar payment_type a valores legibles
        case PAYMENT_TYPE
            when 0 then 'Flex Fare trip'
            when 1 then 'Credit card'
            when 2 then 'Cash'
            when 3 then 'No charge'
            when 4 then 'Dispute'
            when 5 then 'Unknown'
            when 6 then 'Voided trip'
        end as payment_type,
        -- Mantener otras columnas
        VENDORID as vendor_id,
        PASSENGER_COUNT as passenger_count,
        RATECODEID as rate_code_id,
        STORE_AND_FWD_FLAG as store_and_fwd_flag,
        PULocationID as pu_location_id,
        DOLocationID as do_location_id
    from raw
),

cleaned as (
    select *
    from standardized
    where trip_distance >= 0
      and fare_amount >= 0
      and pickup_datetime is not null
      and dropoff_datetime is not null
      and pickup_datetime < dropoff_datetime
      and trip_distance < 200  -- outlier razonable
)

select *
from cleaned