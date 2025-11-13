{{ config(materialized='view') }}

with src as (
  select
    lower(nullif(trim(status), '')) as order_status_name
  from {{ source('SQL_SERVER_DBO','ORDERS') }}
  where coalesce(_fivetran_deleted, false) = false
    and status is not null
)

select
  md5(order_status_name)      as order_status_id,
  order_status_name::varchar  as order_status_name
from src
group by order_status_name
