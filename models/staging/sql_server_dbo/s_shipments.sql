{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='tracking_id'
) }}

with src as (
  select *
  from {{ source('SQL_SERVER_DBO','ORDERS') }}
  where coalesce(_fivetran_deleted,false)=false
    and tracking_id is not null

  {% if is_incremental() %}
    and _fivetran_synced > (select coalesce(max(_fivetran_synced), '1900-01-01'::timestamp_ntz) from {{ this }})
  {% endif %}
),

clean as (
  select
    tracking_id::varchar                  as tracking_id,
    order_id::varchar                     as order_id,
    nullif(trim(shipping_service),'')     as shipping_service,

    -- 👇 Conversión correcta de timestamp con zona a NTZ
    convert_timezone('UTC', created_at)::timestamp_ntz            as created_at,
    convert_timezone('UTC', estimated_delivery_at)::timestamp_ntz as estimated_delivery_at,
    convert_timezone('UTC', delivered_at)::timestamp_ntz          as delivered_at,

    convert_timezone('UTC', _fivetran_synced)                     as _fivetran_synced
  from src
)

select * from clean
