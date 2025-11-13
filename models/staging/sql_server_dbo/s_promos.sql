{{ config(materialized='view') }}

with src as (
  select
    promo_id::varchar                         as promo_id,
    discount::float                           as discount_pct,
    lower(nullif(trim(status), ''))          as status_name
  from {{ source('SQL_SERVER_DBO','PROMOS') }}
  where coalesce(_fivetran_deleted,false)=false
),

enriched as (
  select
    p.promo_id,
    p.discount_pct,
    ps.promo_status_id,
    ps.promo_status_name
  from src p
  left join {{ ref('s_promo_status') }} ps
    on p.status_name = ps.promo_status_name
)

select *
from enriched
