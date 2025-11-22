{{ config(
    materialized='view'
) }}

with src as (
    select
        country,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.ADDRESSES
    where coalesce(_fivetran_deleted, 0) = 0
),

clean as (
    select distinct
        md5(lower(trim(country))) as country_id,
        lower(trim(country))      as country_name,
        last_loaded_utc
    from src
    where country is not null and trim(country) <> ''
)

select *
from clean
