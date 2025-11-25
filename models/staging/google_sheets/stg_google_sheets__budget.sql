with raw_data as (
    select
        product_id,
        -- Solución:
        -- Como MONTH es DATE, solo necesitamos pasarlo a texto con el formato deseado.
        -- Usamos TO_DATE(MONTH) dentro por seguridad (por si fuera TIMESTAMP),
        -- y luego TO_CHAR para obtener 'YYYY-MM'.
        to_char(TO_DATE(MONTH), 'YYYY-MM') as year_month,
        QUANTITY as budget_quantity
    from ALUMNO18_DEV_BRONZE_DB.GOOGLE_SHEETS.BUDGET
),

transformed_data as (
    select
        -- Aseguramos que year_month no sea nulo para la key, o usamos coalesce si fuera necesario
        md5(concat(product_id, '-', year_month)) as surrogate_pd_key,
        product_id,
        year_month,
        budget_quantity,
        current_timestamp() as last_loaded_utc  -- Se agrega la columna last_loaded_utc
    from raw_data
)

select
    surrogate_pd_key,
    product_id,
    year_month,
    budget_quantity,
    last_loaded_utc  -- Aseguramos de incluirla aquí
from transformed_data
