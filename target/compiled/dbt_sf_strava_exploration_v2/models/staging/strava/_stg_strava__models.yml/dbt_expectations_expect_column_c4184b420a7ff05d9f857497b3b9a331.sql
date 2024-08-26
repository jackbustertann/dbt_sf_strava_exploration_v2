





    with grouped_expression as (
    select
        
        
    
  elapsed_time_s >= moving_time_s as expression


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




