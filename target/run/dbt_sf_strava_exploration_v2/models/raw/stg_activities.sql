
  
    

        create or replace transient table STRAVA_DEV.raw.stg_activities
         as
        (with activities as (
    select * from STRAVA_PROD.raw.raw_activities_json
)

select * from activities
        );
      
  