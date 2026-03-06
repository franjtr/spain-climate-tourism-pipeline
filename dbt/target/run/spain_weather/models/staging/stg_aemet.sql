
  create view "spain_weather_tourism"."public"."stg_aemet__dbt_tmp"
    
    
  as (
    with source as (
    select * from "spain_weather_tourism"."public"."raw_aemet_climate"
),

cleaned as (
    select
        indicativo                                    as station_id,
        -- Split fecha "2023-9" into year and month
        split_part(fecha, '-', 1)::int                as year,
        split_part(fecha, '-', 2)::int                as month,
        
        -- Temperature
        tm_mes::numeric                               as avg_temp,
        tm_max::numeric                               as avg_max_temp,
        tm_min::numeric                               as avg_min_temp,
        
        -- Precipitation and sunshine
        p_mes::numeric                                as precipitation,
        e::numeric                                    as sunshine_hours,
        n_des::int                                    as sunny_days,
        n_cub::int                                    as cloudy_days,
        
        -- Other
        hr::numeric                                   as humidity,
        w_med::numeric                                as avg_wind,
        
        extracted_at

    from source
    where
        -- Remove annual summary rows (month 13)
        split_part(fecha, '-', 2)::int between 1 and 12
        -- Only 2019 onwards
        and split_part(fecha, '-', 1)::int >= 2019
)

select * from cleaned
  );