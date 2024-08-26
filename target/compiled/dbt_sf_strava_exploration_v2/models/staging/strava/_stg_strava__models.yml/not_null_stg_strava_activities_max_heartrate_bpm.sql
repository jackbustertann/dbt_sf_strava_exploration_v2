
    
    



select max_heartrate_bpm
from (select * from STRAVA_PROD.staging.stg_strava_activities where has_heartrate = true) dbt_subquery
where max_heartrate_bpm is null


