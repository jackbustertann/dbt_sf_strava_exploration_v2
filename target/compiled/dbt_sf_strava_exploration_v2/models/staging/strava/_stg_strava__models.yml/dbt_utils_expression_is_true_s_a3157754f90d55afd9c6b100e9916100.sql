



select
    1
from STRAVA_PROD.staging.stg_strava__activities

where not(distance_m > 0)

