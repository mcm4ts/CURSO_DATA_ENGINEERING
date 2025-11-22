{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

with src as (
    select
        user_id::varchar                                        as user_id,
        first_name::varchar                                     as first_name,
        last_name::varchar                                      as last_name,
        lower(trim(email))::varchar                             as email,
        phone_number::varchar                                   as phone_number,
        address_id::varchar                                     as address_id,
        CONVERT_TIMEZONE('UTC', created_at)::timestamp_ntz      as created_at_utc,
        CONVERT_TIMEZONE('UTC', updated_at)::timestamp_ntz      as updated_at_utc,
        total_orders::number                                    as total_orders,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.USERS
    where coalesce(_fivetran_deleted, 0) = 0

    {% if is_incremental() %}
      and CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz >
          (
              select coalesce(
                  max(last_loaded_utc),
                  '1900-01-01'::timestamp_ntz
              )
              from {{ this }}
          )
    {% endif %}
)

select *
from src
