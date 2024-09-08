



select
    *
from (select * from STRAVA_STAGING.staging.stg_strava__activities where sport != 'other') dbt_subquery

where not(average_heartrate_bpm > 100)

