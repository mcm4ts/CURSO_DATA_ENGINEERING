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
    and _fivetran_synced >
        (select coalesce(max(_fivetran_synced),'1900-01-01'::timestamp_ntz) from {{ this }})
  {% endif %}
),

typed as (
  select
    order_id::varchar                            as order_id,
    user_id::varchar                             as user_id,
    promo_id::varchar                            as promo_id,
    address_id::varchar                          as address_id,

    lower(nullif(trim(status), ''))              as status_name,
    lower(nullif(trim(shipping_service), ''))    as shipping_service_name,

    round(order_cost,     2)::float              as order_cost,
    round(shipping_cost,  2)::float              as shipping_cost,
    round(order_total,    2)::float              as order_total,

    convert_timezone('UTC', created_at)::timestamp_ntz            as created_at,
    convert_timezone('UTC', estimated_delivery_at)::timestamp_ntz as estimated_delivery_at,
    convert_timezone('UTC', delivered_at)::timestamp_ntz          as delivered_at,

    nullif(trim(tracking_id),'')::varchar        as tracking_id,
    convert_timezone('UTC', _fivetran_synced)    as _fivetran_synced
  from src
),

enriched as (
  select
    o.order_id,
    o.user_id,
    o.promo_id,
    o.address_id,

    os.order_status_id,
    os.order_status_name,

    ss.shipping_service_id,
    ss.shipping_service_name,

    o.order_cost,
    o.shipping_cost,
    o.order_total,
    o.created_at,
    o.estimated_delivery_at,
    o.delivered_at,
    o.tracking_id,
    o._fivetran_synced
  from typed o
  left join {{ ref('s_order_status') }}      os on o.status_name = os.order_status_name
  left join {{ ref('s_shipping_services') }} ss on o.shipping_service_name = ss.shipping_service_name
)

select *
from enriched
