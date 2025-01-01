WITH time_in_heartrate_zones AS (
    SELECT 
        time_in_heartrate_zone_key AS time_in_activity_zone_key,
        activity_key,
        heartrate_zone_key AS activity_zone_key,
        activity_date,
        'heartrate' AS zone_type,
        zone_number, 
        moving_time_in_zone_s
    FROM {{ ref('int_time_in_heartrate_zones' )}}
),

time_in_power_zones AS (
    SELECT 
        time_in_power_zone_key AS time_in_activity_zone_key,
        activity_key,
        power_zone_key AS activity_zone_key,
        activity_date,
        'power' AS zone_type,
        zone_number, 
        moving_time_in_zone_s
    FROM {{ ref('int_time_in_power_zones' )}}
),

time_in_activity_zones AS (

    SELECT *
    FROM time_in_heartrate_zones

    UNION

    SELECT *
    FROM time_in_power_zones

)

SELECT 
    -- surrogate keys,
    {{ dbt_utils.generate_surrogate_key(['time_in_activity_zone_key', 'zone_type']) }} as time_in_activity_zone_key,
    activity_key::varchar AS activity_key,
    {{ dbt_utils.generate_surrogate_key(['activity_zone_key', 'zone_type']) }} as activity_zone_key,
    -- dates
    activity_date::date AS activity_date,
    -- dimensions
    zone_type::varchar AS zone_type,
    zone_number::int AS zone_number,
    -- measures
    moving_time_in_zone_s::int AS moving_time_in_zone_s,
    -- technical meta-data
    CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc
FROM time_in_activity_zones

