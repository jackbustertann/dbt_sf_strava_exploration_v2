select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from STRAVA_PROD.staging.stg_strava__activities

where not(max_heartrate_bpm < 200)


      
    ) dbt_internal_test