{{ config(
    materialized='view'
) }}

with src as (
    select
        zipcode,
        state,
        country,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.ADDRESSES
    where coalesce(_fivetran_deleted, 0) = 0
),

clean as (
    select distinct
        md5(
            zipcode::varchar
            || '|' || lower(trim(state))
            || '|' || lower(trim(country))
        )                                                       as zipcode_id,
        zipcode::number(38,0)                                   as zipcode,
        md5(lower(trim(state)) || '|' || lower(trim(country)))  as state_id,
        md5(lower(trim(country)))                               as country_id,
        last_loaded_utc
    from src
    where zipcode is not null
)

select *
from clean
