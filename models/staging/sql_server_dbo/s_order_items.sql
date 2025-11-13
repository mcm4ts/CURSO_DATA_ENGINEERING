{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['order_id','product_id']) }}

with src as (
  select *
  from {{ source('SQL_SERVER_DBO','ORDER_ITEMS') }}
  where coalesce(_fivetran_deleted,false)=false
)

select
  order_id::varchar   as order_id,
  product_id::varchar as product_id,
  quantity::number    as quantity,
  _fivetran_synced
from src
{% if is_incremental() %}
where _fivetran_synced > (select coalesce(max(_fivetran_synced),'1900-01-01') from {{ this }})
{% endif %}
