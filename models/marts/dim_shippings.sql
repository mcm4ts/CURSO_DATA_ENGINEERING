{{ config(
    materialized = 'table'
) }}

with joined as (
    select
        s.tracking_id,
        s.order_id,
        ss.shipping_service_name,
        s.created_at_utc                as shipment_created_at,
        s.estimated_delivery_at_utc     as shipment_estimated_delivery,
        s.delivered_at_utc              as shipment_delivered_at,
        s.last_loaded_utc::date         as load_date,
        s.last_loaded_utc::time         as load_time
    from {{ ref('stg_sql_server_dbo__shipments') }} s
    left join {{ ref('stg_sql_server_dbo__shipping_services') }} ss
        on s.shipping_service_id = ss.shipping_service_id
)

select *
from joined
