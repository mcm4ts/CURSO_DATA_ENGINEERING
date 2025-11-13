{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id'
) }}

with src as (
  select *
  from {{ source('SQL_SERVER_DBO','ORDERS') }}
  where coalesce(_fivetran_deleted,false)=false
  {% if is_incremental() %}
    and _fivetran_synced > (select coalesce(max(_fivetran_synced), '1900-01-01'::timestamp_ntz) from {{ this }})
  {% endif %}
),

clean as (
  select
    order_id::varchar                          as order_id,
    user_id::varchar                           as user_id,
    promo_id::varchar                          as promo_id,
    address_id::varchar                        as address_id,

    lower(nullif(trim(status),''))::varchar    as status,
    nullif(trim(shipping_service),'')::varchar as shipping_service,

    -- 👇 Aquí el cambio: eliminamos TRY_CAST porque ya son numéricos
    round(order_cost, 2)::float                as order_cost,
    round(shipping_cost, 2)::float             as shipping_cost,
    round(order_total, 2)::float               as order_total,

    -- 👇 Convertimos timestamps a UTC (si vienen con zona)
    convert_timezone('UTC', created_at)::timestamp_ntz             as created_at,
    convert_timezone('UTC', estimated_delivery_at)::timestamp_ntz  as estimated_delivery_at,
    convert_timezone('UTC', delivered_at)::timestamp_ntz           as delivered_at,

    nullif(trim(tracking_id),'')::varchar      as tracking_id,
    convert_timezone('UTC', _fivetran_synced)  as _fivetran_synced
  from src
)

select * from clean
