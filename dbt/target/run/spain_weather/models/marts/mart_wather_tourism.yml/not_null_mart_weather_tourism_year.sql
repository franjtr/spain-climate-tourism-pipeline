
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year
from "spain_weather_tourism"."public"."mart_weather_tourism"
where year is null



  
  
      
    ) dbt_internal_test