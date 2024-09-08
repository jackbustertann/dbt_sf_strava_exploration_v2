
    
    

select
    activity_stream_key as unique_field,
    count(*) as n_records

from STRAVA_DEV.staging.stg_strava_activity_streams
where activity_stream_key is not null
group by activity_stream_key
having count(*) > 1


