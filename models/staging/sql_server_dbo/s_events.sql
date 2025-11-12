{{ config(materialized='incremental', incremental_strategy='merge', unique_key='event_id') }}

with src as (
  select *
  from {{ source('SQL_SERVER_DBO','EVENTS') }}
  where coalesce(_fivetran_deleted,false)=false
),

clean as (
  select
    event_id::varchar        as event_id,
    session_id::varchar      as session_id,
    user_id::varchar         as user_id,
    product_id::varchar      as product_id,
    order_id::varchar        as order_id,
    lower(nullif(trim(event_type),''))::varchar as event_type,
    page_url::varchar        as page_url,
    try_to_timestamp_ntz(created_at) as created_at,
    _fivetran_synced
  from src
)

select * from clean
{% if is_incremental() %}
where _fivetran_synced > (select coalesce(max(_fivetran_synced),'1900-01-01') from {{ this }})
{% endif %};
