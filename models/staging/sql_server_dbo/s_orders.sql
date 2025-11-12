{{ config(materialized='incremental', incremental_strategy='merge', unique_key='order_id') }}

with src as (
  select *
  from {{ source('SQL_SERVER_DBO','ORDERS') }}
  where coalesce(_fivetran_deleted,false)=false
),

clean as (
  select
    order_id::varchar                          as order_id,
    user_id::varchar                           as user_id,
    promo_id::varchar                          as promo_id,
    address_id::varchar                        as address_id,
    lower(nullif(trim(status),''))::varchar    as status,
    nullif(trim(shipping_service),'')::varchar as shipping_service,
    try_to_decimal(order_cost,12,2)::float     as order_cost,
    try_to_decimal(shipping_cost,12,2)::float  as shipping_cost,
    try_to_decimal(order_total,12,2)::float    as order_total,
    try_to_timestamp_ntz(created_at)           as created_at,
    try_to_timestamp_ntz(estimated_delivery_at) as estimated_delivery_at,
    try_to_timestamp_ntz(delivered_at)         as delivered_at,
    nullif(trim(tracking_id),'')::varchar      as tracking_id,
    _fivetran_synced
  from src
)

select * from clean
{% if is_incremental() %}
where _fivetran_synced > (select coalesce(max(_fivetran_synced),'1900-01-01') from {{ this }})
{% endif %};
