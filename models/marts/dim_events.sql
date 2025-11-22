{{ config(
    materialized = 'view'
) }}

with base as (
    select
        event_id,
        event_type_id,
        page_url
    from {{ ref('stg_sql_server_dbo__events') }}
)

select *
from base
