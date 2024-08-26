{{
    config(
        materialized='view' if target.name == 'dev' else 'incremental',
        unique_key='activity_stream_filled_key'
    )
}}

WITH activities AS (
    SELECT activity_key, activity_id, elapsed_time_s
    FROM {{ ref('stg_strava_activities') }}
    {% if target.name == 'dev' %}
    where TO_DATE(loaded_timestamp_utc) >= dateadd('day', -7, current_date)
    {% elif is_incremental() %}
    WHERE loaded_timestamp_utc > (
        SELECT MAX(loaded_timestamp_utc)
        FROM {{ this }}
    )
    {% endif %}
),

activity_streams AS (
    SELECT *
    FROM {{ ref("stg_strava_activity_streams") }}
    WHERE activity_id IN (
        SELECT activity_id
        FROM activities
    )
),

activity_streams_filled AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['activity_id', 'elapsed_time_s']) }} as activity_stream_filled_key,
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
