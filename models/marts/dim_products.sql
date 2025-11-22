{{ config(
    materialized = 'table'
) }}

with base as (
    select
        product_id,
        name          as product_name,
        inventory     as product_inventory,
       
    from {{ ref('stg_sql_server_dbo__products') }}
)

select *
from base
