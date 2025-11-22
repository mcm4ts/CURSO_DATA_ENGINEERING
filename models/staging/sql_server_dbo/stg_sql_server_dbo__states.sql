{{ config(
    materialized='view'
) }}

with src as (
    select
        state,
        country,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.ADDRESSES
    where coalesce(_fivetran_deleted, 0) = 0
),

base as (
    select distinct
        md5(lower(trim(state)) || '|' || lower(trim(country))) as state_id,
        lower(trim(state))                                    as state_name,
        md5(lower(trim(country)))                             as country_id,
        last_loaded_utc
    from src
    where state is not null and trim(state) <> ''
)

select *
from base
