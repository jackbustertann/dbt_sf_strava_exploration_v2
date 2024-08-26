





    with grouped_expression as (
    select
        
        
    
  max_heartrate_bpm >= average_heartrate_bpm as expression


    from STRAVA_PROD.staging.stg_strava_activities
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors




