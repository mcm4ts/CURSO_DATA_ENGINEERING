{{ config(
    materialized='view'
) }}

with src as (
    select
        product_id::varchar                                      as product_id,
        month::date                                              as month,
        quantity::number                                         as quantity,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.GOOGLE_SHEETS.BUDGET
)

select *
from src
