{{ config(materialized='view') }}

with src as (
  select
    lower(nullif(trim(event_type), '')) as event_type_name
  from {{ source('SQL_SERVER_DBO','EVENTS') }}
  where coalesce(_fivetran_deleted, false) = false
    and event_type is not null
)

select
  md5(event_type_name)      as event_type_id,
  event_type_name::varchar  as event_type_name
from src
group by event_type_name
