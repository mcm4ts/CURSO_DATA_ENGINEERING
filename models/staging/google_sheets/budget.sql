{{ config(materialized='view') }}

select
  product_id::varchar as product_id,
  month::date         as month,
  quantity::number    as budget_quantity
from {{ source('SQL_SERVER_DBO','BUDGET') }};
