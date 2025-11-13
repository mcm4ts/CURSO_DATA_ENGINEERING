{{ config(materialized='view') }}

select
  address_id::varchar       as address_id,
  nullif(trim(address),'')  as address_line,
  zipcode::number           as zipcode,
  nullif(trim(state),'')    as state,
  nullif(trim(country),'')  as country
from {{ source('SQL_SERVER_DBO','ADDRESSES') }}
where coalesce(_fivetran_deleted,false)=false
