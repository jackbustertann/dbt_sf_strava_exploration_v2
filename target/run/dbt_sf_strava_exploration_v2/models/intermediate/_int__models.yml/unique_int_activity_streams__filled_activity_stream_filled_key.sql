select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    activity_stream_filled_key as unique_field,
    count(*) as n_records

from STRAVA_PROD.intermediate.int_activity_streams__filled
where activity_stream_filled_key is not null
group by activity_stream_filled_key
having count(*) > 1



      
    ) dbt_internal_test