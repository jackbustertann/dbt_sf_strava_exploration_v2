



select
    1
from (select * from STRAVA_PROD.staging.stg_strava__activities where sport != 'other') dbt_subquery

where not(average_heartrate_bpm > 100)

