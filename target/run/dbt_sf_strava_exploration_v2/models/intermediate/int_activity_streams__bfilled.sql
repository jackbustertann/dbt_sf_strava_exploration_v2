
  create or replace   view STRAVA_DEV.dbt_test.int_activity_streams__bfilled
  
   as (
    SELECT *
FROM STRAVA_DEV.staging.stg_strava_activity_streams
  );

