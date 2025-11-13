{{ config(materialized='view') }}

with src as (
  select
    lower(nullif(trim(status), '')) as promo_status_name
  from {{ source('SQL_SERVER_DBO','PROMOS') }}
  where coalesce(_fivetran_deleted, false) = false
    and status is not null
)

select
  md5(promo_status_name)      as promo_status_id,
  promo_status_name::varchar  as promo_status_name
from src
group by promo_status_name
