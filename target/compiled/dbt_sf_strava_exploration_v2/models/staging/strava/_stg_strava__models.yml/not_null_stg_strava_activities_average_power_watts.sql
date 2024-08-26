
    
    



select average_power_watts
from (select * from STRAVA_PROD.staging.stg_strava_activities where has_power = true) dbt_subquery
where average_power_watts is null


