{{
    config(
        materialized='table' if target.name == 'dev' else 'incremental',
        unique_key='activity_key',
        on_schema_change='fail'
    )
}}

{% set heartrate_zones = [1, 2, 3, 4, 5] %}
{% set power_zones = [1, 2, 3, 4, 5, 6] %}
{% set power_effort_durations = [15, 60, 300, 1200, 3600] %}
{% set effort_distances = [8000, 16000, 32000] %}

WITH ride_activities AS (
    SELECT *
    FROM {{ ref('stg_strava__activities' )}}
    WHERE sport = 'ride'
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
        FROM ride_activities
    )
),

time_in_power_zones_wide AS (
    SELECT *
    FROM {{ ref('int_time_in_power_zones_long_to_wide') }}
    WHERE activity_key IN (
        SELECT activity_key
        FROM ride_activities
    )
),

best_power_efforts_wide AS (
    SELECT *
    FROM {{ ref('int_best_power_efforts_long_to_wide') }}
    WHERE activity_key IN (
        SELECT activity_key
        FROM ride_activities
    )
),

best_distance_efforts_ride_wide AS (
    SELECT *
    FROM {{ ref('int_best_distance_efforts_ride_long_to_wide') }}
    WHERE activity_key IN (
        SELECT activity_key
        FROM ride_activities
    )
)


SELECT 
    -- surrogate keys
    ride_activities.activity_key,
    ride_activities.start_date_key,
    -- natural keys
    ride_activities.activity_id,
    -- dates
    ride_activities.activity_start_timestamp_ntz,
    -- dimensions (non categorical)
    ride_activities.activity_name,
    -- dimensions (categorical)
    ride_activities.sport,
    -- dimensions (boolean)
    ride_activities.is_manual,
    ride_activities.has_heartrate,
    ride_activities.has_power,
    ride_activities.is_indoor,
    ride_activities.is_race,
    -- measures (contextual)
    ride_activities.average_tempature_c,
    ride_activities.min_elevation_m,
    ride_activities.max_elevation_m,
    ride_activities.elevation_gain_m,
    ride_activities.average_cadence_rpm,
    -- measures (volume)
    ride_activities.distance_m,
    ride_activities.elapsed_time_s,
    ride_activities.moving_time_s,
    -- measures (intensity)
    ride_activities.calories_kj,
    ride_activities.average_heartrate_bpm,
    ride_activities.max_heartrate_bpm,
    ride_activities.suffer_score,
    {% for heartrate_zone in heartrate_zones %}
    COALESCE(time_in_heartrate_zones_wide.moving_time_in_zone_{{ heartrate_zone }}_s, 0) AS moving_time_in_heartrate_zone_{{ heartrate_zone }}_s,
    {% endfor %}
    -- measures (performance)
    ride_activities.average_speed_kmhr,
    ride_activities.max_speed_kmhr,
    ride_activities.average_power_watts,
    ride_activities.max_power_watts,
    ride_activities.normalised_power_watts,
    {% for power_zone in power_zones %}
    COALESCE(time_in_power_zones_wide.moving_time_in_zone_{{ power_zone }}_s, 0) AS moving_time_in_power_zone_{{ power_zone }}_s,
    {% endfor %}
    {% for power_effort_duration in power_effort_durations %}
    best_power_efforts_wide.best_power_effort_{{ power_effort_duration }}s_watts,
    {% endfor %}
    {% for effort_distance_m in effort_distances %}
    {% set effort_distance_km = (effort_distance_m / 1000)|int %}
    best_distance_efforts_ride_wide.best_distance_effort_{{ effort_distance_km }}km_kmhr,
    {% endfor %}
    -- technical meta-data
    CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc
FROM ride_activities
LEFT JOIN time_in_heartrate_zones_wide
    ON ride_activities.activity_key = time_in_heartrate_zones_wide.activity_key
LEFT JOIN time_in_power_zones_wide
    ON ride_activities.activity_key = time_in_power_zones_wide.activity_key
LEFT JOIN best_power_efforts_wide
    ON ride_activities.activity_key = best_power_efforts_wide.activity_key
LEFT JOIN best_distance_efforts_ride_wide
    ON ride_activities.activity_key = best_distance_efforts_ride_wide.activity_key