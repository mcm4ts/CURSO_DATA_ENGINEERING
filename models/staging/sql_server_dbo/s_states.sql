{{ config(materialized='view') }}

with src as (
  select
    upper(trim(state))   as state_name,
    upper(trim(country)) as country_name
  from {{ source('SQL_SERVER_DBO','ADDRESSES') }}
  where coalesce(_fivetran_deleted, false) = false
    and state   is not null
    and country is not null
),

countries as (
  select
    md5(country_name)     as country_id,
    country_name
  from src
  group by country_name
)

select
  md5(state_name || '|' || country_name) as state_id,
  s.state_name::varchar                  as state_name,
  c.country_id                           as country_id
from src s
join countries c using (country_name)
group by state_id, state_name, country_id
