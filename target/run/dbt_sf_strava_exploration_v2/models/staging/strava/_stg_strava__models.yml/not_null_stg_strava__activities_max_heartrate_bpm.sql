select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select max_heartrate_bpm
from (select * from STRAVA_PROD.staging.stg_strava__activities where has_heartrate = true) dbt_subquery
where max_heartrate_bpm is null



      
    ) dbt_internal_test