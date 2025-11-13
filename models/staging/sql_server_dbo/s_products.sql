{{ config(materialized='view') }}

select
  product_id::varchar            as product_id,
  nullif(trim(name),'')          as name,
  round(price, 2)::float         as price_usd,
  inventory::number              as inventory
from {{ source('SQL_SERVER_DBO','PRODUCTS') }}
where coalesce(_fivetran_deleted,false)=false
