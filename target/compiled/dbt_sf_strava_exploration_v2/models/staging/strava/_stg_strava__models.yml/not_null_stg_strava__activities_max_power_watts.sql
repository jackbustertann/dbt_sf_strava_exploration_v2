
    
    



select max_power_watts
from (select * from STRAVA_PROD.staging.stg_strava__activities where has_power = true) dbt_subquery
where max_power_watts is null

