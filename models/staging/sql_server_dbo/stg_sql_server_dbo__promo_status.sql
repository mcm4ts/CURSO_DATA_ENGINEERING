{{ config(
    materialized='view'
) }}

with src as (
    select
        status,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.PROMOS
    where coalesce(_fivetran_deleted, 0) = 0
),

clean as (
    select distinct
        md5(lower(trim(status))) as promo_status_id,
        lower(trim(status))      as promo_status_name,
        last_loaded_utc
    from src
    where status is not null and trim(status) <> ''
)

select *
from clean
