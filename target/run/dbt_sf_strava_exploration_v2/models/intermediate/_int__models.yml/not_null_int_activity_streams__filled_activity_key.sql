select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select activity_key
from STRAVA_PROD.intermediate.int_activity_streams__filled
where activity_key is null



      
    ) dbt_internal_test