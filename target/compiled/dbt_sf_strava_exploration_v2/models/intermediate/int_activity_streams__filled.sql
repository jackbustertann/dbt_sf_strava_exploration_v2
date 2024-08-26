

WITH activities AS (
    SELECT activity_key, activity_id, elapsed_time_s
    FROM STRAVA_STAGING.staging.stg_strava_activities
    
    WHERE loaded_timestamp_utc > (
        SELECT MAX(loaded_timestamp_utc)
        FROM STRAVA_STAGING.intermediate.int_activity_streams__filled
    )
    
),

activity_streams AS (
    SELECT *
    FROM STRAVA_STAGING.staging.stg_strava_activity_streams
    WHERE activity_id IN (
        SELECT activity_id
        FROM activities
    )
),

activity_streams_filled AS (
    SELECT
        md5(cast(coalesce(cast(activity_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(elapsed_time_s as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as activity_stream_filled_key,
        CASE 
            WHEN activity_stream_key IS NULL THEN false
            ELSE true 
        END AS is_recorded,
        *
    FROM (
        SELECT 
            activities.activity_id AS activity_id,
            time.value AS elapsed_time_s
        FROM activities, 
            LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, activities.elapsed_time_s)) time
    ) AS elapsed_time_filled
    NATURAL LEFT JOIN activity_streams
)

SELECT 
    -- surrogate keys
    activity_stream_filled_key,
    activity_stream_key,
    activity_key,
    -- natural keys
    activity_id,
    elapsed_time_s,
    -- dimensions (boolean)
    is_moving,
    is_recorded,
    -- measures (contextual)
    latitude,
    longitude,
    temperature_c,
    grade_percent,
    elevation_m,
    cadence_rpm,
    -- measures (volume)
    distance_m,
    -- measures (intensity)
    heartrate_bpm,
    -- measures (performance)
    speed_ms,
    power_watts,
    -- technical meta-data
    CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc
FROM activity_streams_filled