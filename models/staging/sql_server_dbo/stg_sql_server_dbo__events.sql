{{ config(
    materialized='incremental',
    unique_key='event_id'
) }}

with src as (
    select
        event_id::varchar                                        as event_id,
        page_url::varchar                                        as page_url,
        event_type::varchar                                      as event_type_raw,
        user_id::varchar                                         as user_id,
        product_id::varchar                                      as product_id,
        session_id::varchar                                      as session_id,
        created_at::timestamp_ntz                                as created_at_utc,
        order_id::varchar                                        as order_id,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz as last_loaded_utc
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.EVENTS

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
),

joined as (
    select
        e.event_id,
        e.session_id,
        e.user_id,
        e.product_id,
        e.order_id,
        et.event_type_id,
        et.event_type_name,
        e.page_url,
        e.created_at_utc,
        e.last_loaded_utc
    from src e
    left join {{ ref('stg_sql_server_dbo__event_types') }} et
        on lower(trim(e.event_type_raw)) = et.event_type_name
)

select *
from joined
