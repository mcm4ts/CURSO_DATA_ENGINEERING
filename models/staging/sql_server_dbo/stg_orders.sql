{{ config(materialized='view') }}

with src as (
  select *
  from {{ source('SQL_SERVER_DBO', 'ORDERS') }}
  where coalesce(_FIVETRAN_DELETED, false) = false
),

-- 1) Tipado seguro y normalización básica
typed as (
  select
    order_id::varchar                                  as order_id,
    user_id::varchar                                   as user_id,
    promo_id::varchar                                  as promo_id,
    address_id::varchar                                as address_id,

    try_to_timestamp_ntz(created_at)                   as created_at,
    try_to_timestamp_ntz(delivered_at)                 as delivered_at,
    try_to_timestamp_ntz(estimated_delivery_at)        as estimated_delivery_at,

    try_to_decimal(order_cost,      12,2)::float       as order_cost,
    try_to_decimal(shipping_cost,   12,2)::float       as shipping_cost,
    try_to_decimal(order_total,     12,2)::float       as order_total,

    nullif(trim(shipping_service), '')::varchar        as shipping_service,
    nullif(trim(tracking_id), '')::varchar             as tracking_id,
    lower(trim(status))::varchar                       as status,

    _fivetran_synced
  from src
),

-- 2) Deduplicación por order_id (quedarse con la versión más reciente)
dedup as (
  select
    t.*,
    row_number() over (
      partition by t.order_id
      order by t._fivetran_synced desc nulls last
    ) as rn
  from typed t
)

select
  order_id,
  user_id,
  promo_id,
  address_id,
  created_at,
  delivered_at,
  estimated_delivery_at,
  order_cost,
  shipping_cost,
  order_total,
  shipping_service,
  tracking_id,
  status,
  /* (opcional) columnas derivadas para tu modelo OLAP */
  cast(created_at as date) as created_at_date_utc,
  cast(created_at as time) as created_at_time_utc,
  current_date()           as date_load_utc,
  current_time()           as time_load_utc
from dedup
where rn = 1;
