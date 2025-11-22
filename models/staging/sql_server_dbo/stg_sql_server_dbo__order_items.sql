{{ config(
    materialized='incremental',
    unique_key=['order_id', 'product_id']
) }}

with src as (
    select
        order_id::varchar                                        as order_id,
        product_id::varchar                                      as product_id,
        quantity::number                                         as quantity,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.ORDER_ITEMS

    {% if is_incremental() %}
      where CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz >
            (
                select coalesce(
                    max(last_loaded_utc),
                    '1900-01-01'::timestamp_ntz
                )
                from {{ this }}
            )
    {% endif %}
)

select *
from src
