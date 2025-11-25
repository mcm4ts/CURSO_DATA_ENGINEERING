{% snapshot products_snapshot %}

    {{
        config(
          target_schema=target.schema,
          unique_key='product_id',
          strategy='timestamp',
          updated_at='last_loaded_utc'
        )
    }}

    select
        product_id,
        -- Reemplazamos nulos en price_usd por 0
        coalesce(price_usd, 0) as price_usd,
        inventory,
        name,
        last_loaded_utc
    from {{ ref('stg_sql_server_dbo__products') }}

{% endsnapshot %}
