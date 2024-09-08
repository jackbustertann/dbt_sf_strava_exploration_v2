



select
    1
from STRAVA_PROD.staging.stg_strava__activities

where not(max_heartrate_bpm < 200)

