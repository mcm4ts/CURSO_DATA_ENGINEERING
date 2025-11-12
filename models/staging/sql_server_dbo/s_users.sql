{{ config(materialized='incremental', incremental_strategy='merge', unique_key='user_id') }}

with src as (
  select *
  from {{ source('SQL_SERVER_DBO','USERS') }}
  where coalesce(_fivetran_deleted,false)=false
),

clean as (
  select
    user_id::varchar            as user_id,
    nullif(trim(first_name),'') as first_name,
    nullif(trim(last_name),'')  as last_name,
    lower(trim(email))::varchar as email,
    nullif(trim(phone_number),'') as phone_number,
    address_id::varchar         as address_id,
    try_to_timestamp_ntz(created_at) as created_at,
    try_to_timestamp_ntz(updated_at) as updated_at,
    total_orders::number        as total_orders,
    _fivetran_synced
  from src
)

select * from clean
{% if is_incremental() %}
where _fivetran_synced > (select coalesce(max(_fivetran_synced),'1900-01-01') from {{ this }})
{% endif %};
