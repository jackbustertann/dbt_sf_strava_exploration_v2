





    with grouped_expression as (
    select
        
        
    
  max_power_watts >= average_power_watts as expression


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




