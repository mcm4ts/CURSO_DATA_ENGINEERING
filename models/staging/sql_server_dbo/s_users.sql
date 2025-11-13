{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='user_id'
) }}

with src as (
  select *
  from {{ source('SQL_SERVER_DBO','USERS') }}
  where coalesce(_fivetran_deleted,false)=false
  {% if is_incremental() %}
    and _fivetran_synced >
        (select coalesce(max(_fivetran_synced),'1900-01-01'::timestamp_ntz) from {{ this }})
  {% endif %}
),

clean as (
  select
    user_id::varchar              as user_id,
    nullif(trim(first_name),'')   as first_name,
    nullif(trim(last_name),'')    as last_name,
    lower(trim(email))::varchar   as email,
    nullif(trim(phone_number),'') as phone_number,
    address_id::varchar           as address_id,
    convert_timezone('UTC', created_at)::timestamp_ntz as created_at,
    convert_timezone('UTC', updated_at)::timestamp_ntz as updated_at,
    total_orders::number          as total_orders,
    convert_timezone('UTC', _fivetran_synced)          as _fivetran_synced
  from src
)

select * from clean
