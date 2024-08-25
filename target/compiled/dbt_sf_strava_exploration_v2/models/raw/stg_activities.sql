with activities as (
    select * from STRAVA_PROD.raw.raw_activities_json
)

select * from activities