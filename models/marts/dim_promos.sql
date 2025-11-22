{{ config(
    materialized = 'table'
) }}

with joined as (
    select
        p.promo_id,
        p.promo_name,
        p.discount_dollars              as promo_discount_usd,
        ps.promo_status_name            as promo_status,
        
    from {{ ref('stg_sql_server_dbo__promos') }} p
    left join {{ ref('stg_sql_server_dbo__promo_status') }} ps
        on p.promo_status_id = ps.promo_status_id
)

select *
from joined
