with weather as (
    select * from {{ ref('stg_aemet') }}
),

tourism as (
    select * from {{ ref('stg_ine_tourism') }}
),

provinces as (
    select * from {{ ref('province_mapping') }}
),

-- Join weather with province mapping to get province names
weather_by_province as (
    select
        p.province_name,
        p.ccaa,
        w.year,
        w.month,
        -- Mean imputation for temperature
        coalesce(
            w.avg_temp, 
            avg(w.avg_temp) over(partition by p.province_name, w.month)
        ) as avg_temp,
        -- Imputation for max and min average temperature
        coalesce(w.avg_max_temp, avg(w.avg_max_temp) over(partition by p.province_name, w.month)) as avg_max_temp,
        coalesce(w.avg_min_temp, avg(w.avg_min_temp) over(partition by p.province_name, w.month)) as avg_min_temp,
        w.precipitation,
        w.sunshine_hours,
        w.sunny_days,
        w.cloudy_days,
        w.humidity,
        w.avg_wind
    from weather w
    inner join provinces p
        on w.station_id = p.station_id::text
),

-- Pivot tourism: one row per province/year/month with viajeros and pernoctaciones
tourism_pivoted as (
    select
        province_name,
        year,
        month,
        max(case when metric = 'viajeros' then value end)        as tourists,
        max(case when metric = 'pernoctaciones' then value end)  as overnight_stays

    from tourism
    group by province_name, year, month
),

-- Final join
final as (
    select
        w.province_name,
        w.ccaa,
        w.year,
        w.month,
        w.avg_temp,
        w.avg_max_temp,
        w.avg_min_temp,
        w.precipitation,
        w.sunshine_hours,
        w.sunny_days,
        w.cloudy_days,
        w.humidity,
        w.avg_wind,
        t.tourists,
        t.overnight_stays,
        round(t.overnight_stays / nullif(t.tourists, 0), 2)  as avg_stay_days

    from weather_by_province w
    left join tourism_pivoted t
        on w.province_name = t.province_name
        and w.year = t.year
        and w.month = t.month
)

select * from final