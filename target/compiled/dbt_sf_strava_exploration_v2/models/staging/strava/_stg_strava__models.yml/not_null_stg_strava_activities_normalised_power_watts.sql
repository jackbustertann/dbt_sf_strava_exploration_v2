
    
    



select normalised_power_watts
from (select * from STRAVA_PROD.staging.stg_strava_activities where has_power = true) dbt_subquery
where normalised_power_watts is null


