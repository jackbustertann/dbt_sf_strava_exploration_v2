select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from STRAVA_PROD.staging.stg_strava_activities

where not(distance_m > 0)


      
    ) dbt_internal_test