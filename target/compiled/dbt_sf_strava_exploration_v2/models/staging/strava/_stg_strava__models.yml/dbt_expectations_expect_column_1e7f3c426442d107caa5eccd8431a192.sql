





    with grouped_expression as (
    select
        
        
    
  max_speed_ms >= average_speed_ms as expression


    from STRAVA_PROD.staging.stg_strava__activities
    

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




