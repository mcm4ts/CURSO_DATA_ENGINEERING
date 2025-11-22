{{ config(
    materialized='view'
) }}

with src as (
    select
        event_type,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.EVENTS
),

clean as (
    select distinct
        md5(lower(trim(event_type))) as event_type_id,
        lower(trim(event_type))      as event_type_name,
        last_loaded_utc
    from src
    where event_type is not null and trim(event_type) <> ''
)

select *
from clean
