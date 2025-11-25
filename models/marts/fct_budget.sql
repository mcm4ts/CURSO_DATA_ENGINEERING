with base as (
    select
        surrogate_pd_key::varchar as surrogate_pd_key,
        product_id::varchar as product_id,
        year_month::varchar as year_month,
        budget_quantity
    -- IMPORTANTE: Usamos ref() para que dbt entienda que fct depende de stg.
    -- El nombre dentro de las comillas DEBE ser el nombre del archivo del paso anterior (sin .sql)
    from {{ ref('stg_google_sheets__budget') }}
)

select 
    surrogate_pd_key,
    product_id,
    year_month,
    budget_quantity
from base