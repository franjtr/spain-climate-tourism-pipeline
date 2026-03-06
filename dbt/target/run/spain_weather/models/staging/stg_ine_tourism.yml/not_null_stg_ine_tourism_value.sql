
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select value
from "spain_weather_tourism"."public"."stg_ine_tourism"
where value is null



  
  
      
    ) dbt_internal_test