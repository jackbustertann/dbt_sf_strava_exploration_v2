
    
    



select average_heartrate_bpm
from (select * from STRAVA_PROD.staging.stg_strava__activities where has_heartrate = true) dbt_subquery
where average_heartrate_bpm is null

