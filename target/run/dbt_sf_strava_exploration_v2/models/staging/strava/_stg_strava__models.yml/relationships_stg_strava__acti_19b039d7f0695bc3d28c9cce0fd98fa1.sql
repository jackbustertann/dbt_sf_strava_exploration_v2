select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select activity_key as from_field
    from STRAVA_PROD.staging.stg_strava__activity_streams
    where activity_key is not null
),

parent as (
    select activity_key as to_field
    from STRAVA_PROD.staging.stg_strava__activities
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test