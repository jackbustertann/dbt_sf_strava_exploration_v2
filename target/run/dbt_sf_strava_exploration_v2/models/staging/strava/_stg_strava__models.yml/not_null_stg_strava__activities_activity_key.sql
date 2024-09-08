select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select activity_key
from STRAVA_PROD.staging.stg_strava__activities
where activity_key is null



      
    ) dbt_internal_test