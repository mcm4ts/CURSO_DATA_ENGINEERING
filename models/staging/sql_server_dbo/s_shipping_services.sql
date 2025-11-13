{{ config(materialized='view') }}

with src as (
  select
    lower(nullif(trim(shipping_service), '')) as shipping_service_name
  from {{ source('SQL_SERVER_DBO','ORDERS') }}
  where coalesce(_fivetran_deleted, false) = false
    and shipping_service is not null
)

select
  md5(shipping_service_name)        as shipping_service_id,
  shipping_service_name::varchar    as shipping_service_name
from src
group by shipping_service_name
