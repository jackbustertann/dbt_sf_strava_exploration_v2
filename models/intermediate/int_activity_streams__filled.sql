{{
    config(
        materialized='view' if target.name == 'dev' else 'incremental',
        unique_key='activity_stream_filled_key',
        on_schema_change='fail'
    )
}}

WITH activities AS (
    SELECT activity_key, elapsed_time_s
    FROM {{ ref('stg_strava__activities') }}
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
    FROM {{ ref("stg_strava__activity_streams") }}
    WHERE activity_key IN (
        SELECT activity_key
        FROM activities
    )
),

elapsed_time_filled AS (
    SELECT 
        activities.activity_key,
        CAST(time.value AS INT) AS elapsed_time_filled_s,
        {{ dbt_utils.generate_surrogate_key(['activity_key', 'elapsed_time_filled_s']) }} as activity_stream_filled_key
    FROM activities, 
        LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, activities.elapsed_time_s)) time
),

activity_streams_filled AS (
    SELECT
        elapsed_time_filled.activity_stream_filled_key,
        elapsed_time_filled.activity_key,
        elapsed_time_filled.elapsed_time_filled_s AS elapsed_time_s,
        activity_streams.* EXCLUDE (activity_key, elapsed_time_s),
        CASE 
            WHEN activity_stream_key IS NULL THEN false
            ELSE true 
        END AS is_recorded
    FROM elapsed_time_filled
    LEFT JOIN activity_streams
    ON elapsed_time_filled.activity_stream_filled_key = activity_streams.activity_stream_key
)

SELECT 
    -- surrogate keys
    activity_stream_filled_key,
    activity_stream_key,
    activity_key,
    -- natural keys
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
