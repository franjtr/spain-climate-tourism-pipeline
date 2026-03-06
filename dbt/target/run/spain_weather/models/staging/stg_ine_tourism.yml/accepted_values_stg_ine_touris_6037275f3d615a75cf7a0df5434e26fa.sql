
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test