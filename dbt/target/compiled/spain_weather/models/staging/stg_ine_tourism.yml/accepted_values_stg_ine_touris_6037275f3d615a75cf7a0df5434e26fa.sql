
    
    

with all_values as (

    select
        metric as value_field,
        count(*) as n_records

    from "spain_weather_tourism"."public"."stg_ine_tourism"
    group by metric

)

select *
from all_values
where value_field not in (
    'viajeros','pernoctaciones'
)


