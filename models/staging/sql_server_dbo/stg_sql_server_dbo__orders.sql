{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with src as (
    select
        order_id::varchar                                        as order_id,
        user_id::varchar                                         as user_id,
        address_id::varchar                                      as address_id,
        shipping_service,
        promo_id,
        status,
        shipping_cost,
        order_cost,
        order_total,
        created_at,
        estimated_delivery_at,
        delivered_at,
        CONVERT_TIMEZONE('UTC', created_at)           ::timestamp_ntz as created_at_utc,
        CONVERT_TIMEZONE('UTC', estimated_delivery_at)::timestamp_ntz as estimated_delivery_at_utc,
        CONVERT_TIMEZONE('UTC', delivered_at)         ::timestamp_ntz as delivered_at_utc,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)     ::timestamp_ntz as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.ORDERS
    where coalesce(_fivetran_deleted, 0) = 0

    {% if is_incremental() %}
      and CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz >
          (
              select coalesce(
                  max(last_loaded_utc),
                  '1900-01-01'::timestamp_ntz
              ) 
              from {{ this }}
          )
    {% endif %}
),

clean as (
    select
        order_id,
        user_id,
        address_id,
        created_at_utc,
        estimated_delivery_at_utc,
        delivered_at_utc,
        md5(lower(trim(replace(shipping_service,'-','_'))))           as shipping_service_id,
        to_decimal(order_total,  12, 2)                               as order_total_usd,
        to_decimal(order_cost,   12, 2)                               as order_cost_usd,
        to_decimal(shipping_cost,12, 2)                               as shipping_cost_usd,
        md5(lower(trim(replace(coalesce(promo_id,'no_promo'),'-','_')))) as promo_id,
        md5(lower(trim(status)))                                      as order_status_id,
        last_loaded_utc
    from src
)

select *
from clean
