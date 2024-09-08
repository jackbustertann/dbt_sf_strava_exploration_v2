select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      




  
  


with windowed as (

    select
        activity_key, 
        elapsed_time_s,
        lag(elapsed_time_s) over (
            partition by activity_key
            order by elapsed_time_s
        ) as previous_elapsed_time_s
    from STRAVA_PROD.intermediate.int_activity_streams__filled
),

validation_errors as (
    select
        *
    from windowed
    
    where not(elapsed_time_s = previous_elapsed_time_s + 1)
    
)

select *
from validation_errors


      
    ) dbt_internal_test