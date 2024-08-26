



select
    1
from STRAVA_PROD.staging.stg_strava_activities

where not(distance_m > 0)

