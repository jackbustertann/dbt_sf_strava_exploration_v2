select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select activity_stream_key
from STRAVA_DEV.staging.stg_strava_activity_streams
where activity_stream_key is null



      
    ) dbt_internal_test