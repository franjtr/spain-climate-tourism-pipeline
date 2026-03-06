
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select metric
from "spain_weather_tourism"."public"."stg_ine_tourism"
where metric is null



  
  
      
    ) dbt_internal_test