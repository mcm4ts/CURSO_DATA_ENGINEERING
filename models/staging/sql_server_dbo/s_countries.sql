{{ config(materialized='view') }}

with src as (
  select
    upper(trim(country)) as country_name
  from {{ source('SQL_SERVER_DBO','ADDRESSES') }}
  where coalesce(_fivetran_deleted, false) = false
    and country is not null
)

select
  md5(country_name)      as country_id,
  country_name::varchar  as country_name
from src
group by country_name
