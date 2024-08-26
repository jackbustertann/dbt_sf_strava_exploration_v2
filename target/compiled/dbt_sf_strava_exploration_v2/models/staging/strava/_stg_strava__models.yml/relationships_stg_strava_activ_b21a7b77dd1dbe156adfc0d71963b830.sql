
    
    

with child as (
    select activity_id as from_field
    from STRAVA_PROD.staging.stg_strava_activity_streams
    where activity_id is not null
),

parent as (
    select activity_id as to_field
    from STRAVA_PROD.staging.stg_strava_activities
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


