{{ config(
    materialized='view'
) }}

with src as (
    select
        product_id::varchar as product_id,
        coalesce(to_decimal(price_dollars, 12, 2), 0) as price_usd,  -- Reemplazamos nulos por 0
        name::varchar as name,
        inventory::number as inventory,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.PRODUCTS
    -- si tienes _fivetran_deleted, podrías filtrar:
    -- where coalesce(_fivetran_deleted, 0) = 0
)

select *
from src
