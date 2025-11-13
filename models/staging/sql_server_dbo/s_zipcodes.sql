{{ config(materialized='view') }}

with src as (
  select
    zipcode::number       as zipcode,
    upper(trim(state))    as state_name,
    upper(trim(country))  as country_name
  from {{ source('SQL_SERVER_DBO','ADDRESSES') }}
  where coalesce(_fivetran_deleted, false) = false
    and zipcode is not null
),

countries as (
  select
    md5(country_name)     as country_id,
    country_name
  from (select distinct country_name from src)
),

states as (
  select
    md5(state_name || '|' || country_name) as state_id,
    state_name,
    country_name
  from (select distinct state_name, country_name from src)
)

select
  s.zipcode                   as zipcode,
  st.state_id                 as state_id,
  c.country_id                as country_id
from src s
join states    st using (state_name, country_name)
join countries c  using (country_name)
group by zipcode, state_id, country_id
