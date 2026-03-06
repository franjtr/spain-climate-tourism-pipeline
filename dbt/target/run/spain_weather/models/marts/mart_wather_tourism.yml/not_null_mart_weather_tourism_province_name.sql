
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select province_name
from "spain_weather_tourism"."public"."mart_weather_tourism"
where province_name is null



  
  
      
    ) dbt_internal_test