{{ config(
    materialized='table'
) }}

with order_items as (
    select
        order_id,
        product_id,
        quantity,
        last_loaded_utc
    from {{ ref('stg_sql_server_dbo__order_items') }}
),

orders as (
    select
        order_id,
        user_id,
        promo_id,
        created_at_utc,
        shipping_cost_usd
    from {{ ref('stg_sql_server_dbo__orders') }}
),

shipments as (
    select
        order_id,
        tracking_id
    from {{ ref('stg_sql_server_dbo__shipments') }}
),

prices as (
    select
        product_id,
        coalesce(price_usd, 0) as price_usd,  -- Reemplazamos los precios nulos por 0
        inventory,
        name,
        last_loaded_utc as valid_from,
        current_timestamp() as valid_to
    from {{ ref('products_snapshot') }}  -- Referenciamos el snapshot de precios
),

order_qty as (
    select
        order_id,
        sum(quantity) as total_quantity
    from order_items
    group by order_id
),

joined as (
    select
        oi.order_id,
        oi.product_id,
        o.user_id,
        o.promo_id,
        sh.tracking_id,
        o.created_at_utc::date as date,  -- FK a DIM_DATES
        oi.quantity as quantity,
        p.price_usd,
        (oi.quantity * coalesce(p.price_usd, 0)) as order_subcost_usd,  -- Aplicamos coalesce para evitar nulos
        (o.shipping_cost_usd * oi.quantity / nullif(oq.total_quantity, 0)) as shipping_subcost_usd,
        (
            (oi.quantity * coalesce(p.price_usd, 0))
            + (o.shipping_cost_usd * oi.quantity / nullif(oq.total_quantity,0))
        ) as total_subcost_usd,  -- Aplicamos coalesce aquí también
        oi.last_loaded_utc::date as date_load_utc
    from order_items oi
    left join orders o
        on oi.order_id = o.order_id
    left join shipments sh
        on oi.order_id = sh.order_id
    left join order_qty oq
        on oi.order_id = oq.order_id
    left join prices p
        on oi.product_id = p.product_id
       and o.created_at_utc between p.valid_from and p.valid_to  -- Verificamos que el precio esté dentro del rango de validez
),

final as (
    select
        md5(
            coalesce(order_id,'') || '|' ||
            coalesce(product_id,'') || '|' ||
            coalesce(user_id,'') || '|' ||
            coalesce(promo_id,'') || '|' ||
            coalesce(tracking_id,'') || '|' ||
            to_char(date, 'YYYY-MM-DD')
        ) as surrogate_op_key,
        order_id,
        product_id,
        user_id,
        promo_id,
        tracking_id,
        date,  -- FK a DIM_DATES
        quantity,
        price_usd,
        order_subcost_usd,
        shipping_subcost_usd,
        total_subcost_usd,
        date_load_utc
    from joined
)

select *
from final
