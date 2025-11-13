{{ config(materialized='view') }}

with src as (
  select
    address_id::varchar             as address_id,
    nullif(trim(address), '')       as address_line,
    zipcode::number                 as zipcode,
    upper(trim(state))              as state_name,
    upper(trim(country))            as country_name
  from {{ source('SQL_SERVER_DBO','ADDRESSES') }}
  where coalesce(_fivetran_deleted, false) = false
),

enriched as (
  select
    a.address_id,
    a.address_line,
    z.zipcode,
    z.state_id,
    z.country_id
  from src a
  left join {{ ref('s_zipcodes') }} z
    on a.zipcode = z.zipcode
)

select *
from enriched
