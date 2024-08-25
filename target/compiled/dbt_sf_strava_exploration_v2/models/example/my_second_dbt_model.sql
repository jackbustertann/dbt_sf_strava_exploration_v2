-- Use the `ref` function to select from other models

select *
from STRAVA_DEV.dbt_test.my_first_dbt_model
where id = 1