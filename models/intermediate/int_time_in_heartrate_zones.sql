-- time in heartrate zones (cycling + running)

-- methodology
-- - filter cycling activities, with heartrate data
-- - get activity streams for activities, filtering out static records
-- - get heartrate zones for each activity
-- - calculate (moving) time in heartrate zones from activity streams for each activity

-- caveats
-- - moving time across all heartrate zones does not cover all moving time due to gaps in streams

-- tests
-- uniqueness: time in heartrate zone key (activity key, heartrate zone number)
-- nullability: N/A
-- allowed values: N/A
-- referential integrity: N/A

-- future considerations
-- - add custom unit test to check full coverage of zones for each activity
-- - reduce complexity of joins

WITH activities AS (
    SELECT 
        activity_key,
        start_date_key,
        moving_time_s
    FROM {{ ref('stg_strava__activities')}}
    WHERE has_heartrate
),

activity_streams AS (
    SELECT 
        activity_stream_key,
        activity_key,
        heartrate_bpm
    FROM {{ ref('stg_strava__activity_streams') }}
    WHERE activity_key IN (
        SELECT DISTINCT activity_key
        FROM activities
    )
        AND is_moving
),

dates AS (
    SELECT 
        date_key,
        date_day
    FROM {{ ref('dim_dates') }}
),

zone_bounds AS (
    SELECT 
        heartrate_zone_key,
        start_date,
        end_date,
        zone_number,
        lower_bound,
        upper_bound
    FROM {{ ref('int_heartrate_zones') }}
),

activity_zones AS (
    SELECT 
        activities.activity_key, 
        TO_DATE(activities.start_date_key, 'yyyymmdd') AS activity_date,
        zone_bounds.heartrate_zone_key,
        zone_bounds.zone_number,
        zone_bounds.lower_bound, 
        zone_bounds.upper_bound
    FROM activities
    JOIN dates
        ON activities.start_date_key = dates.date_key
    JOIN zone_bounds
        ON dates.date_day > zone_bounds.start_date
        AND dates.date_day <= zone_bounds.end_date
),

activity_time_in_zones AS (
    SELECT 
        activity_zones.activity_key,
        activity_zones.heartrate_zone_key,
        activity_zones.activity_date,
        activity_zones.zone_number,
        COUNT(activity_streams.activity_stream_key) AS moving_time_in_zone
    FROM activity_streams
    RIGHT JOIN activity_zones
        ON activity_streams.activity_key = activity_zones.activity_key
            AND ((activity_streams.heartrate_bpm > activity_zones.lower_bound OR activity_zones.lower_bound = 0) 
            AND (activity_streams.heartrate_bpm <= activity_zones.upper_bound OR activity_zones.upper_bound = -1))
    GROUP BY 1, 2, 3, 4
)

SELECT 
    -- surrogate keys
    {{ dbt_utils.generate_surrogate_key(['activity_key', 'zone_number']) }} as time_in_heartrate_zone_key,
    activity_key::varchar AS activity_key,
    heartrate_zone_key::varchar AS heartrate_zone_key,
    -- dates
    activity_date::date AS activity_date,
    -- dimensions
    zone_number::int AS zone_number,
    -- measures
    moving_time_in_zone::int AS moving_time_in_zone_s,
    -- technical meta-data
    CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc
FROM activity_time_in_zones

