{{ config(
    materialized = 'table'
) }}

-- 1) Generamos un rango de fechas (por ejemplo, de 2018 a 2030)
with calendar as (

    select
        dateadd(day, seq4(), to_date('2018-01-01')) as date
    from table(generator(rowcount => 5000))  -- ~13 años

),

-- 2) Calculamos todos los atributos de fecha
dim_dates as (

    select
        date                                        as date,              -- PK
        day(date)                                  as day,               -- día del mes (1-31)

        /* weekday: número de día de la semana
           Usamos ISO: 1 = lunes, 7 = domingo */
        dayofweekiso(date)                         as weekday,

        /* weekday_str: nombre del día */
        initcap(dayname(date))                     as weekday_str,       -- 'Monday', 'Tuesday', ...

        month(date)                                as month,             -- número de mes (1-12)
        initcap(monthname(date))                   as month_str,         -- 'January', ...

        year(date)                                 as year,
        weekofyear(date)                           as week,              -- semana del año
        quarter(date)                              as quarter            -- 1,2,3,4
    from calendar
)

select *
from dim_dates