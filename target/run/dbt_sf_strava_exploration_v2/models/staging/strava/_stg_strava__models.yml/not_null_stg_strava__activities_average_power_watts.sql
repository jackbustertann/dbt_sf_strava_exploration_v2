select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select average_power_watts
from (select * from STRAVA_PROD.staging.stg_strava__activities where has_power = true) dbt_subquery
where average_power_watts is null



      
    ) dbt_internal_test