{{
    config(
        materialized='table' if target.name == 'dev' else 'incremental',
        unique_key='activity_key',
        on_schema_change='fail'
    )
}}

{% set heartrate_zones = [1, 2, 3, 4, 5] %}
{% set effort_distances = [1000, 1600, 3000, 5000, 8000, 10000, 16000, 21100] %}

WITH run_activities AS (
    SELECT *
    FROM {{ ref('stg_strava__activities' )}}
    WHERE sport = 'run'
    {% if is_incremental() %}
        AND loaded_timestamp_utc > (
            SELECT MAX(loaded_timestamp_utc)
            FROM {{ this }}
        ) -- TODO: move casting of last modified timestamp to raw table
    {% endif %}
),

time_in_heartrate_zones_wide AS (
    SELECT *
    FROM {{ ref('int_time_in_heartrate_zones_long_to_wide') }}
    WHERE activity_key IN (
        SELECT activity_key
        FROM run_activities
    )
),

best_distance_efforts_run_wide AS (
    SELECT *
    FROM {{ ref('int_best_distance_efforts_run_long_to_wide') }}
    WHERE activity_key IN (
        SELECT activity_key
        FROM run_activities
    )
)

SELECT 
    -- surrogate keys
    run_activities.activity_key,
    run_activities.start_date_key,
    -- natural keys
    run_activities.activity_id,
    -- dates
    run_activities.activity_start_timestamp_ntz,
    -- dimensions (non categorical)
    run_activities.activity_name,
    -- dimensions (categorical)
    run_activities.sport,
    -- dimensions (boolean)
    run_activities.is_manual,
    run_activities.has_heartrate,
    run_activities.has_power,
    run_activities.is_indoor,
    run_activities.is_race,
    -- measures (contextual)
    run_activities.average_tempature_c,
    run_activities.min_elevation_m,
    run_activities.max_elevation_m,
    run_activities.elevation_gain_m,
    run_activities.average_cadence_rpm,
    -- measures (volume)
    run_activities.distance_m,
    run_activities.elapsed_time_s,
    run_activities.moving_time_s,
    -- measures (intensity)
    run_activities.calories_kj,
    run_activities.average_heartrate_bpm,
    run_activities.max_heartrate_bpm,
    run_activities.suffer_score,
    {% for heartrate_zone in heartrate_zones %}
    COALESCE(time_in_heartrate_zones_wide.moving_time_in_zone_{{ heartrate_zone }}_s, 0) AS moving_time_in_heartrate_zone_{{ heartrate_zone }}_s,
    {% endfor %}
    -- measures (performance)
    run_activities.average_speed_kmhr,
    run_activities.max_speed_kmhr,
    {% for effort_distance_m in effort_distances %}
    best_distance_efforts_run_wide.best_distance_effort_{{ effort_distance_m }}m_s,
    {% endfor %}
    -- technical meta-data
    CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc
FROM run_activities
LEFT JOIN time_in_heartrate_zones_wide
    ON run_activities.activity_key = time_in_heartrate_zones_wide.activity_key
LEFT JOIN best_distance_efforts_run_wide
    ON run_activities.activity_key = best_distance_efforts_run_wide.activity_key