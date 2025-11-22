{{ config(
    materialized='view'
) }}

with src as (
    select
        promo_id,
        discount                                                    as discount_dollars,
        status,
        lower(replace(trim(replace(promo_id, '-', '_')), ' ', '_')) as promo_name,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz    as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.PROMOS
    where coalesce(_fivetran_deleted, 0) = 0
),

base as (
    select
        md5(promo_name)                      as promo_id,
        promo_name,
        discount_dollars                     as discount_dollars,
        md5(lower(trim(status)))             as promo_status_id,
        last_loaded_utc
    from src

    union all

    select
        md5('no_promo')                      as promo_id,
        'no_promo'                           as promo_name,
        0                                    as discount_dollars,
        md5('inactive')                      as promo_status_id,
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::timestamp_ntz as last_loaded_utc
)

select *
from base
