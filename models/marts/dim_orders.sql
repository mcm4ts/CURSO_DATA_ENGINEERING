{{ config(
    materialized = 'table'
) }}

with joined as (
    select
        o.order_id,
        o.created_at_utc             as order_created_at,
        os.order_status_name         as order_status,
        o.last_loaded_utc::date      as load_date,
    from {{ ref('stg_sql_server_dbo__orders') }} o
    left join {{ ref('stg_sql_server_dbo__order_status') }} os
        on o.order_status_id = os.order_status_id
)

select *
from joined
