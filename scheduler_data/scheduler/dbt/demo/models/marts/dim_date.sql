{{ config(
    materialized='table'
) }}

with date_spine as (
    -- Generar fechas para un rango razonable (asume que dbt_utils está instalado)
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2015-01-01' as date)",
        end_date="dateadd(year, 1, current_date())"
    ) }}
),

final as (
    select
        to_varchar(date_day, 'YYYYMMDD')::int as date_sk, 
        date_day as full_date,
        dayname(date_day) as day_name,
        dayofweek(date_day) as day_of_week,
        monthname(date_day) as month_name,
        year(date_day) as year,
        case when dayofweek(date_day) in (6, 7) then true else false end as is_weekend
    from date_spine
)

select * from final