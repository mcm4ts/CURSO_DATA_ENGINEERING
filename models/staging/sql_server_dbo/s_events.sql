{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id'
) }}

with src as (
  select *
  from {{ source('SQL_SERVER_DBO','EVENTS') }}
  where coalesce(_fivetran_deleted,false)=false
  {% if is_incremental() %}
    and _fivetran_synced >
        (select coalesce(max(_fivetran_synced),'1900-01-01'::timestamp_ntz) from {{ this }})
  {% endif %}
),

typed as (
  select
    event_id::varchar                           as event_id,
    session_id::varchar                         as session_id,
    user_id::varchar                            as user_id,
    product_id::varchar                         as product_id,
    order_id::varchar                           as order_id,
    lower(nullif(trim(event_type),''))          as event_type_name,
    page_url::varchar                           as page_url,
    created_at::timestamp_ntz                   as created_at,
    _fivetran_synced
  from src
),

enriched as (
  select
    e.event_id,
    e.session_id,
    e.user_id,
    e.product_id,
    e.order_id,
    et.event_type_id,
    et.event_type_name,
    e.page_url,
    e.created_at,
    e._fivetran_synced
  from typed e
  left join {{ ref('s_event_types') }} et
    on e.event_type_name = et.event_type_name
)

select *
from enriched
