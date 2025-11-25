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
        inventory,
        price_usd,
        name,
        last_loaded_utc  -- Asegúrate de que 'last_loaded_utc' esté correctamente incluido
    from {{ ref('stg_sql_server_dbo__products') }}

{% endsnapshot %}
