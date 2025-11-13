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
    and _fivetran_synced >
        (select coalesce(max(_fivetran_synced),'1900-01-01'::timestamp_ntz) from {{ this }})
  {% endif %}
),

typed as (
  select
    tracking_id::varchar                      as tracking_id,
    order_id::varchar                         as order_id,
    lower(nullif(trim(shipping_service),''))  as shipping_service_name,
    convert_timezone('UTC', created_at)::timestamp_ntz            as created_at,
    convert_timezone('UTC', estimated_delivery_at)::timestamp_ntz as estimated_delivery_at,
    convert_timezone('UTC', delivered_at)::timestamp_ntz          as delivered_at,
    convert_timezone('UTC', _fivetran_synced)                     as _fivetran_synced
  from src
),

enriched as (
  select
    s.tracking_id,
    s.order_id,
    ss.shipping_service_id,
    ss.shipping_service_name,
    s.created_at,
    s.estimated_delivery_at,
    s.delivered_at,
    s._fivetran_synced
  from typed s
  left join {{ ref('s_shipping_services') }} ss
    on s.shipping_service_name = ss.shipping_service_name
)

select *
from enriched
