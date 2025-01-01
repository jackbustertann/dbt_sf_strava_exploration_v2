{% set dob = '1997-06-19' %}

WITH age_changes AS (
    SELECT
        TO_DATE(date_key, 'yyyymmdd') AS birthday_date,
        DATEDIFF(year, TO_DATE('{{ dob }}', 'yyyy-mm-dd'), TO_DATE(date_key, 'yyyymmdd')) AS age_years
    FROM {{ ref("dim_dates") }}
    WHERE TO_VARCHAR(TO_DATE(date_key, 'yyyymmdd'), 'mm-dd') = TO_VARCHAR(TO_DATE('{{ dob }}', 'yyyy-mm-dd'), 'mm-dd')
),

max_heartrate_with_date_ranges AS (
    SELECT 
        birthday_date AS start_date,
        LEAST(DATEADD(year, 1, birthday_date), CONVERT_TIMEZONE('UTC', current_timestamp)::date) AS end_date,
        ROUND(211 - (0.64 * age_years), 0) AS estimated_max_heartrate_bpm
    FROM age_changes
)

SELECT 
    -- surrogate keys
    {{ dbt_utils.generate_surrogate_key(['start_date']) }} as max_heartrate_key,
    -- dates
    start_date::date AS start_date,
    end_date::date AS end_date,
    -- measures
    estimated_max_heartrate_bpm::int AS estimated_max_heartrate_bpm,
    -- technical meta-data
    CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc
FROM max_heartrate_with_date_ranges