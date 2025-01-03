-- ftp (cycling only)

-- methodology
-- - filter cycling activities which meet criteria for a race / time-trial
-- - get 20 minute best power efforts for activities, group by activity date
-- - filter activities with the highest 20 minute best power efforts over past year
-- - estimate ftp based on 20 minute best power efforts and get date ranges

-- caveats
-- - indoor activities lead to a slight over-estimation in ftp vs outdoor activities
-- - ftp is less accurate for periods with small number of race / time trial activities

-- tests
-- uniqueness: ftp_key
-- nullability: N/A
-- allowed values: N/A
-- referential integrity: N/A

-- future considerations
-- - research other methodologies for calculating ftp which consider more activities and shorter effort durations

WITH eligible_rides AS (
    SELECT
        activity_key,
        TO_DATE(start_date_key, 'yyyymmdd') AS activity_date
    FROM {{ ref("stg_strava__activities") }}
    WHERE sport = 'ride'
        AND (
            (LOWER(activity_name) LIKE '%race:%') 
            OR (LOWER(activity_name) LIKE '% tt%')
            OR (LOWER(activity_name) LIKE '%regents park%')
            OR (LOWER(activity_name) LIKE '%richmond park%')
        ) -- TODO: create more robust race flag in activities staging
),

best_20_minute_efforts_for_eligible_rides AS (
    SELECT 
        activity_key,
        average_power_watts AS best_20_min_power_watts
    FROM {{ ref("int_best_power_efforts") }}
    WHERE effort_duration_s = 1200
        AND activity_key IN (
            SELECT activity_key
            FROM eligible_rides
        )
),

new_ftp_efforts AS (
    SELECT 
        rides.activity_key,
        rides.activity_date,
        best_efforts.best_20_min_power_watts
    FROM eligible_rides rides
    JOIN best_20_minute_efforts_for_eligible_rides best_efforts
        ON rides.activity_key = best_efforts.activity_key
    QUALIFY best_efforts.best_20_min_power_watts = MAX(best_efforts.best_20_min_power_watts) OVER(
        ORDER BY rides.activity_date RANGE BETWEEN INTERVAL '365 days' PRECEDING AND CURRENT ROW
    )
),

ftp_changes_with_date_ranges AS (
    SELECT 
        activity_key,
        activity_date AS start_date,
        LEAD(start_date, 1, CONVERT_TIMEZONE('UTC', current_timestamp)::date) OVER(ORDER BY start_date) AS end_date,
        best_20_min_power_watts * 0.95 AS ftp_watts
    FROM new_ftp_efforts
)

SELECT 
    -- surrogate keys
    {{ dbt_utils.generate_surrogate_key(['start_date']) }} as ftp_key,
    activity_key::varchar AS activity_key,
    -- dates
    start_date::date AS start_date,
    end_date::date AS end_date,
    -- measures
    ftp_watts::float AS ftp_watts,
    -- technical meta-data
    CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc
FROM ftp_changes_with_date_ranges