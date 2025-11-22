{{ config(
    materialized='view'
) }}

with src as (
    select
        shipping_service,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.ORDERS
),

clean as (
    select distinct
        md5(lower(replace(trim(replace(shipping_service, '-', '_')), ' ', '_'))) as shipping_service_id,
        case 
            when shipping_service is null or trim(shipping_service) = '' 
                then 'no_shipping_service_selected'
            else lower(replace(trim(replace(shipping_service, '-', '_')), ' ', '_'))
        end as shipping_service_name,
        last_loaded_utc
    from src
)

select *
from clean
