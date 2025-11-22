{{ config(
    materialized='incremental',
    unique_key='address_id'
) }}

with src as (
    select
        address_id::varchar                                          as address_id,
        zipcode,
        state,
        country,
        address::varchar                                             as address,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz     as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.ADDRESSES
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
),

clean as (
    select
        address_id,
        md5(
            zipcode::varchar
            || '|' || lower(trim(state))
            || '|' || lower(trim(country))
        )                   as zipcode_id,
        address,
        last_loaded_utc
    from src
)

select *
from clean
