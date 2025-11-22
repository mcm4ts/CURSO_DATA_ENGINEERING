{{ config(
    materialized='incremental',
    unique_key=['product_id', 'valid_from']
) }}

with src as (
    select
        product_id::varchar                                      as product_id,
        to_decimal(price, 12, 2)                                 as price_usd,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz as change_ts
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.PRODUCTS
    -- si quieres ignorar borrados (si existiera la columna):
    -- where coalesce(_fivetran_deleted, 0) = 0
),

ordered as (
    select
        product_id,
        price_usd,
        change_ts,
        lag(price_usd) over (partition by product_id order by change_ts) as prev_price
    from src
),

change_points as (
    select
        product_id,
        price_usd,
        change_ts as valid_from
    from ordered
    where prev_price is null
       or price_usd <> prev_price
),

scd_ranges as (
    select
        product_id,
        price_usd,
        valid_from,
        lead(valid_from) over (partition by product_id order by valid_from) as next_from
    from change_points
)

select
    product_id,
    price_usd,
    valid_from,
    next_from                                      as valid_to,
    iff(next_from is null, true, false)           as is_current,
    valid_from                                    as _version_synced
from scd_ranges
