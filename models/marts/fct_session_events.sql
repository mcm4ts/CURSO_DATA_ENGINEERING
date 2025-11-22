{{ config(
    materialized = 'table'
) }}

with base as (
    select
        event_id,
        session_id,
        user_id,
        order_id,
        product_id,
        created_at_utc,
        last_loaded_utc
    from {{ ref('stg_sql_server_dbo__events') }}
),

final as (
    select
        md5(
            coalesce(session_id,'') || '|' ||
            coalesce(event_id,'') || '|' ||
            to_char(created_at_utc::date, 'YYYY-MM-DD')
        )                                  as surrogate_se_key,
        session_id,
        event_id,
        user_id,
        order_id,
        product_id,
        created_at_utc::date               as date,          -- FK a DIM_DATES
        last_loaded_utc::date              as date_load_utc
    from base
)

select *
from final
