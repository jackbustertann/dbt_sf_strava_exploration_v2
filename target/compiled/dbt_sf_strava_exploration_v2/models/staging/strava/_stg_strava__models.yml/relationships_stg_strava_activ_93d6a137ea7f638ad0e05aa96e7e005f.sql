
    
    

with child as (
    select activity_key as from_field
    from STRAVA_DEV.staging.stg_strava_activity_streams
    where activity_key is not null
),

parent as (
    select activity_key as to_field
    from STRAVA_DEV.staging.stg_strava_activities
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


