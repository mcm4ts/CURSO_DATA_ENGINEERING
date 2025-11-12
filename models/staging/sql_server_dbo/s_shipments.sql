{{ config(materialized='incremental', incremental_strategy='merge', unique_key='tracking_id') }}

with src as (
  select *
  from {{ source('SQL_SERVER_DBO','ORDERS') }}
  where coalesce(_fivetran_deleted,false)=false
    and tracking_id is not null
),

clean as (
  select
    tracking_id::varchar                 as tracking_id,
    order_id::varchar                    as order_id,
    nullif(trim(shipping_service),'')    as shipping_service,
    try_to_timestamp_ntz(created_at)     as created_at,
    try_to_timestamp_ntz(estimated_delivery_at) as estimated_delivery_at,
    try_to_timestamp_ntz(delivered_at)   as delivered_at,
    _fivetran_synced
  from src
)

select * from clean
{% if is_incremental() %}
where _fivetran_synced > (select coalesce(max(_fivetran_synced),'1900-01-01') from {{ this }})
{% endif %};
