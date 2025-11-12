{{ config(materialized='view') }}

select
  promo_id::varchar          as promo_id,
  discount::number           as discount_pct,
  lower(nullif(trim(status),''))::varchar as status
from {{ source('SQL_SERVER_DBO','PROMOS') }}
where coalesce(_fivetran_deleted,false)=false;
