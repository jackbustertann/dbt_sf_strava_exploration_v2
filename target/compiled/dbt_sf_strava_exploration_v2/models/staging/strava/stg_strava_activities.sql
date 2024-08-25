-- steps
-- extract required keys from JSON [x]
-- rename columns [x]
-- cast data-types [x]
-- convert units [x]
-- null/zero default value imputations (e.g. zero power) [x]
-- case when imputations (e.g. in-accurate heartrate/power readings, manual uploads) [x]
-- basic calculated fields (e.g. sport, is_indoor, is_race) [x]
-- add file meta-data (created/extracted/loaded ts, source system) [x]
-- generate surrogate keys [x]



with activities_raw as (
    select *
    from STRAVA_PROD.raw.strava_activities
    
    WHERE metadata_last_modified > (
        SELECT MAX(extracted_timestamp)
        FROM STRAVA_DEV.staging.stg_strava_activities
    )
    
)


, activities_extracted_and_renamed AS (
    SELECT 
    get(VALUE, 'id') AS activity_id,
    get(VALUE, 'name') AS activity_name,
    get(VALUE, 'type') AS activity_type,
    get(VALUE, 'has_heartrate') AS has_heartrate,
    get(VALUE, 'device_watts') AS has_power,
    get(VALUE, 'manual') AS is_manual,
    get(VALUE, 'distance') AS distance_m,
    get(VALUE, 'elev_low') AS min_elevation_m,
    get(VALUE, 'elev_high') AS max_elevation_m,
    get(VALUE, 'total_elevation_gain') AS elevation_gain_m,
    get(VALUE, 'moving_time') AS moving_time_s,
    get(VALUE, 'elapsed_time') AS elapsed_time_s,
    get(VALUE, 'average_speed') AS average_speed_ms,
    get(VALUE, 'max_speed') AS max_speed_ms,
    get(VALUE, 'average_cadence') AS average_cadence_rpm,
    get(VALUE, 'average_temp') AS average_tempature_c,
    get(VALUE, 'kilojoules') AS calories_kj,
    get(VALUE, 'average_heartrate') AS average_heartrate_bpm,
    get(VALUE, 'max_heartrate') AS max_heartrate_bpm,
    get(VALUE, 'suffer_score') AS suffer_score,
    get(VALUE, 'average_watts') AS average_power_watts,
    get(VALUE, 'weighted_average_watts') AS normalised_power_watts,
    get(VALUE, 'max_watts') AS max_power_watts,
    get(VALUE, 'start_date_local') AS activity_start_datetime_ltz,
    
        metadata_filename,
        metadata_last_modified,
    FROM activities_raw, LATERAL FLATTEN(INPUT => RAW_JSON)
)

, activities_casted AS (
    SELECT 
    activity_id::int AS activity_id,
    activity_name::string AS activity_name,
    activity_type::string AS activity_type,
    has_heartrate::boolean AS has_heartrate,
    has_power::boolean AS has_power,
    is_manual::boolean AS is_manual,
    distance_m::float AS distance_m,
    min_elevation_m::float AS min_elevation_m,
    max_elevation_m::float AS max_elevation_m,
    elevation_gain_m::float AS elevation_gain_m,
    moving_time_s::float AS moving_time_s,
    elapsed_time_s::float AS elapsed_time_s,
    average_speed_ms::float AS average_speed_ms,
    max_speed_ms::float AS max_speed_ms,
    average_cadence_rpm::float AS average_cadence_rpm,
    average_tempature_c::float AS average_tempature_c,
    calories_kj::float AS calories_kj,
    average_heartrate_bpm::float AS average_heartrate_bpm,
    max_heartrate_bpm::float AS max_heartrate_bpm,
    suffer_score::float AS suffer_score,
    average_power_watts::float AS average_power_watts,
    normalised_power_watts::float AS normalised_power_watts,
    max_power_watts::float AS max_power_watts,
    TO_TIMESTAMP_NTZ(activity_start_datetime_ltz) AS activity_start_datetime_ltz,
    
        metadata_filename,
        metadata_last_modified,
    FROM activities_extracted_and_renamed
)

, activities_with_default_value_imputations AS (
    SELECT 
    IFNULL(activity_id, -1) as activity_id,
    activity_name,
    activity_type,
    IFNULL(has_heartrate, False) as has_heartrate,
    IFNULL(has_power, False) as has_power,
    IFNULL(is_manual, False) as is_manual,
    IFF(distance_m = 0, null, distance_m) AS distance_m,
    IFF(min_elevation_m = 0, null, min_elevation_m) AS min_elevation_m,
    IFF(max_elevation_m = 0, null, max_elevation_m) AS max_elevation_m,
    IFF(elevation_gain_m = 0, null, elevation_gain_m) AS elevation_gain_m,
    IFF(moving_time_s = 0, null, moving_time_s) AS moving_time_s,
    IFF(elapsed_time_s = 0, null, elapsed_time_s) AS elapsed_time_s,
    IFF(average_speed_ms = 0, null, average_speed_ms) AS average_speed_ms,
    IFF(max_speed_ms = 0, null, max_speed_ms) AS max_speed_ms,
    IFF(average_cadence_rpm = 0, null, average_cadence_rpm) AS average_cadence_rpm,
    IFF(average_tempature_c = 0, null, average_tempature_c) AS average_tempature_c,
    IFF(calories_kj = 0, null, calories_kj) AS calories_kj,
    IFF(average_heartrate_bpm = 0, null, average_heartrate_bpm) AS average_heartrate_bpm,
    IFF(max_heartrate_bpm = 0, null, max_heartrate_bpm) AS max_heartrate_bpm,
    IFF(suffer_score = 0, null, suffer_score) AS suffer_score,
    IFF(average_power_watts = 0, null, average_power_watts) AS average_power_watts,
    IFF(normalised_power_watts = 0, null, normalised_power_watts) AS normalised_power_watts,
    IFF(max_power_watts = 0, null, max_power_watts) AS max_power_watts,
    IFNULL(activity_start_datetime_ltz, TIMESTAMP_NTZ_FROM_PARTS(1900, 1, 1, 00, 00, 00)) as activity_start_datetime_ltz,
    
        metadata_filename,
        metadata_last_modified,
    FROM activities_casted
)

, activities_with_case_when_imputations AS (
    SELECT 
        * REPLACE(
        CASE
            WHEN activity_id IN (1985636421, 2028511938, 2001078643, 2271688574, 8169509675, 3061139289, 2691579673) THEN false
            ELSE has_heartrate
        END AS has_heartrate,
        CASE 
            WHEN NOT has_heartrate THEN NULL
            ELSE average_heartrate_bpm
        END AS average_heartrate_bpm,
        CASE 
            WHEN NOT has_heartrate THEN NULL
            ELSE max_heartrate_bpm
        END AS max_heartrate_bpm,
        CASE 
            WHEN NOT has_heartrate THEN NULL
            ELSE calories_kj
        END AS calories_kj,
        CASE 
            WHEN NOT has_heartrate THEN NULL
            ELSE suffer_score
        END AS suffer_score,
        CASE
            WHEN DATE_TRUNC('day', activity_start_datetime_ltz) <= TO_TIMESTAMP_NTZ('2022-10-23') THEN false
            WHEN activity_id IN (11766807666, 8465793563, 9994884386, 8465794237) THEN false
            ELSE has_power
        END AS has_power,
        CASE 
            WHEN NOT has_power THEN NULL
            ELSE average_power_watts
        END AS average_power_watts,
        CASE 
            WHEN NOT has_power THEN NULL
            ELSE normalised_power_watts
        END AS normalised_power_watts,
        CASE 
            WHEN NOT has_power THEN NULL
            ELSE max_power_watts
        END AS max_power_watts
        )
    FROM activities_with_default_value_imputations
)

, activities_with_calculated_fields AS (
SELECT 
    *,
    CASE 
        WHEN activity_type ILIKE '%run%' THEN 'run'
        WHEN activity_type ILIKE '%ride%' THEN 'ride'
        ELSE 'other'
    END AS sport,
    CASE
        WHEN activity_type ILIKE '%virtual%' THEN true
        WHEN activity_name ILIKE ANY ('%treadmill%', '%indoor%', '%zwift%', '%spin%', '%digme%', '%pyscle%') THEN true 
        ELSE false
    END AS is_indoor,
    CASE 
        WHEN activity_name ILIKE '%race%' THEN true
        ELSE false
    END AS is_race
FROM activities_with_case_when_imputations
)

, activities_with_keys_and_metadata AS (
    SELECT 
        *,
        metadata_last_modified AS extracted_timestamp,
        md5(cast(coalesce(cast(activity_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as activity_key,
        TO_VARCHAR(activity_start_datetime_ltz, 'yyyymmdd') as start_date_key,
        'strava_api' AS record_source
    FROM activities_with_calculated_fields
)

SELECT 
    -- surrogate keys
    activity_key,
    start_date_key,
    -- natural keys
    activity_id,
    -- dates
    activity_start_datetime_ltz,
    -- dimensions (non categorical)
    activity_name,
    -- dimensions (categorical)
    sport,
    -- dimensions (boolean)
    is_manual,
    has_heartrate,
    has_power,
    is_indoor,
    is_race,
    -- measures (contextual)
    average_tempature_c,
    min_elevation_m,
    max_elevation_m,
    elevation_gain_m,
    average_cadence_rpm,
    -- measures (volume)
    distance_m,
    elapsed_time_s,
    moving_time_s,
    -- measures (intensity)
    calories_kj,
    average_heartrate_bpm,
    max_heartrate_bpm,
    suffer_score,
    -- measures (performance)
    average_speed_ms,
    max_speed_ms,
    average_power_watts,
    max_power_watts,
    normalised_power_watts,
    -- technical meta-data
    extracted_timestamp,
    record_source
FROM activities_with_keys_and_metadata