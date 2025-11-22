{{ config(
    materialized='incremental',
    unique_key='tracking_id'
) }}

with src as (
    select
        tracking_id::varchar                                     as tracking_id,
        order_id::varchar                                        as order_id,
        shipping_service,
        created_at,
        estimated_delivery_at,
        delivered_at,
        CONVERT_TIMEZONE('UTC', created_at)           ::timestamp_ntz as created_at_utc,
        CONVERT_TIMEZONE('UTC', estimated_delivery_at)::timestamp_ntz as estimated_delivery_at_utc,
        CONVERT_TIMEZONE('UTC', delivered_at)         ::timestamp_ntz as delivered_at_utc,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)     ::timestamp_ntz as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.ORDERS
    where coalesce(_fivetran_deleted, 0) = 0
      and tracking_id is not null

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
        tracking_id,
        order_id,
        md5(lower(replace(trim(replace(shipping_service, '-', '_')), ' ', '_'))) as shipping_service_id,
        created_at_utc,
        estimated_delivery_at_utc,
        delivered_at_utc,
        last_loaded_utc
    from src
)

select *
from clean
