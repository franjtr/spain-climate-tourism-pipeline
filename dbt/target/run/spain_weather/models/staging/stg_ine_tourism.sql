
  create view "spain_weather_tourism"."public"."stg_ine_tourism__dbt_tmp"
    
    
  as (
    with source as (
    select * from "spain_weather_tourism"."public"."raw_ine_tourism"
),

cleaned as (
    select
        provincia                                          as province_name,
        metric,
        year::int                                          as year,
        replace(month, 'M', '')::int                       as month,
        value::numeric                                     as value,
        extracted_at

    from source
    where
        -- Remove null values marked as secret by INE
        secret is null
        and value is not null
        -- Only 2019 onwards
        and year::int >= 2019
)

select * from cleaned
  );