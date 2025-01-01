WITH heartrate_zones AS (
    SELECT 
        heartrate_zone_key AS activity_zone_key,
        start_date, 
        end_date,
        'heartrate' AS zone_type,
        zone_number, 
        lower_bound,
        upper_bound,
        1 = RANK() OVER(ORDER BY start_date DESC) AS is_current
    FROM {{ ref('int_heartrate_zones' )}}
),

power_zones AS (
    SELECT
        power_zone_key AS activity_zone_key,
        start_date, 
        end_date,
        'power' AS zone_type,
        zone_number, 
        lower_bound,
        upper_bound,
        1 = RANK() OVER(ORDER BY start_date DESC) AS is_current
    FROM {{ ref('int_power_zones' )}}
),

activity_zones AS (

    SELECT *
    FROM heartrate_zones

    UNION

    SELECT *
    FROM power_zones

)

SELECT 
    -- surrogate keys,
    {{ dbt_utils.generate_surrogate_key(['activity_zone_key', 'zone_type']) }} as activity_zone_key,
    -- dates
    start_date::date AS start_date,
    end_date::date AS end_date,
    --booleans
    is_current::boolean AS is_current,
    -- dimensions
    zone_type::varchar AS zone_type,
    zone_number::int AS zone_number,
    -- measures
    lower_bound::int AS lower_bound,
    upper_bound::int AS upper_bound,
    -- technical meta-data
    CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc
FROM activity_zones
ORDER BY start_date DESC, zone_number

