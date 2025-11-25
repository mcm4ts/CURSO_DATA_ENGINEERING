{{ config(
    materialized='view'
) }}

with src as (
    select
        zipcode,
        state,
        country,
        CONVERT_TIMEZONE('UTC', _fivetran_synced)::timestamp_ntz as last_loaded_utc -- Convierte la fecha de sincronización al estándar UTC.
    from ALUMNO18_DEV_BRONZE_DB.SQL_SERVER_DBO.ADDRESSES
    where coalesce(_fivetran_deleted, 0) = 0 -- Elimina registros marcados como borrados por Fivetran.
),

-- Normalización y generación de surrogate keys: •	zipcode no es único dentro del estado o país (es decir, podrías tener varias instancias de zipcode con el mismo valor pero en diferentes estados o países).
clean as (
    select distinct
        md5(zipcode::varchar || '|' || lower(trim(state)) || '|' || lower(trim(country))) as zipcode_id, -- Generación de surrogate key
        zipcode::number(38,0) as zipcode,  -- Zipcode como número
        states.state_id,  -- Este campo debería ser traído de la tabla `states` para hacer la relación
        src.last_loaded_utc  -- Especificamos que `last_loaded_utc` proviene de la tabla `src`
    from src
    left join ALUMNO18_DEV_SILVER_DB.dbt_jparejanieto_dbt_jparejanieto_SILVER.stg_sql_server_dbo__states as states  -- Relación con la tabla de estados
    on lower(trim(state)) = lower(trim(states.state_name))  -- Hacemos la relación por estado
    where zipcode is not null
)

select *
from clean
