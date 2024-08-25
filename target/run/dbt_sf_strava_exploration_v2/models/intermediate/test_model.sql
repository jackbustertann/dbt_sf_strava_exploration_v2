
  create or replace   view STRAVA_DEV.dbt_test.test_model
  
   as (
    SELECT *
FROM STRAVA_DEV.staging.stg_strava_activities
LIMIT 10
  );

