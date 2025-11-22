{{ config(
    materialized = 'view'
) }}

with base as (
    select
        product_id,
        quantity                              as budget_quantity,
        extract(month from month)::number(2,0) as budget_month_number,
    from {{ ref('stg_google_sheets__budget') }}
)

select *
from base
