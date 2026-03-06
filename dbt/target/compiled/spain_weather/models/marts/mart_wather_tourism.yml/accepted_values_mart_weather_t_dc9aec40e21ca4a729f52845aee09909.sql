
    
    

with all_values as (

    select
        month as value_field,
        count(*) as n_records

    from "spain_weather_tourism"."public"."mart_weather_tourism"
    group by month

)

select *
from all_values
where value_field not in (
    '1','2','3','4','5','6','7','8','9','10','11','12'
)


