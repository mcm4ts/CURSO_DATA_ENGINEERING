{{ config(
    materialized = 'table'
) }}

with joined as (
    select
        u.user_id,
        u.first_name,
        u.last_name,
        u.email,
        u.phone_number,
        a.address,
        z.zipcode,
        s.state_name   as state,
        c.country_name as country,
        u.created_at_utc as user_created_at,
        u.updated_at_utc as user_updated_at,
        u.last_loaded_utc::date as load_date,
    from {{ ref('stg_sql_server_dbo__users') }} u
    left join {{ ref('stg_sql_server_dbo__addresses') }} a
        on u.address_id = a.address_id
    left join {{ ref('stg_sql_server_dbo__zipcodes') }} z
        on a.zipcode_id = z.zipcode_id
    left join {{ ref('stg_sql_server_dbo__states') }} s
        on z.state_id = s.state_id
    left join {{ ref('stg_sql_server_dbo__countries') }} c
        on s.country_id = c.country_id
)

select *
from joined
