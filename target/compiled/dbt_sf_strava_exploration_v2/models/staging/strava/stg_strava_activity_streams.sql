-- steps
-- extract required keys from JSON [x]
-- rename columns [x]
-- cast data-types [x]
-- convert units [x]
-- null/zero imputations [x]
-- special case imputations [x]
-- basic calculated fields (e.g. extract lat/long from array) [x]
-- add file meta-data (created/extracted/loaded ts, source system) [x]
-- generate surrogate keys [x]



with activity_streams_raw as (
    select *
    from STRAVA_PROD.raw.strava_activity_streams
    
    WHERE metadata_last_modified > (
        SELECT MAX(extracted_timestamp)
        FROM STRAVA_DEV.staging.stg_strava_activity_streams
    )
    
)



, activity_streams_extracted_and_renamed AS (
    SELECT 
    FILTER(RAW_JSON, a -> a:type = 'time')[0]['data'][time.index] AS elapsed_time_s,
    FILTER(RAW_JSON, a -> a:type = 'moving')[0]['data'][time.index] AS is_moving,
    FILTER(RAW_JSON, a -> a:type = 'latlng')[0]['data'][time.index] AS latlng_array,
    FILTER(RAW_JSON, a -> a:type = 'heartrate')[0]['data'][time.index] AS heartrate_bpm,
    FILTER(RAW_JSON, a -> a:type = 'cadence')[0]['data'][time.index] AS cadence_rpm,
    FILTER(RAW_JSON, a -> a:type = 'temp')[0]['data'][time.index] AS temperature_c,
    FILTER(RAW_JSON, a -> a:type = 'distance')[0]['data'][time.index] AS distance_m,
    FILTER(RAW_JSON, a -> a:type = 'watts')[0]['data'][time.index] AS power_watts,
    FILTER(RAW_JSON, a -> a:type = 'velocity_smooth')[0]['data'][time.index] AS speed_ms,
    FILTER(RAW_JSON, a -> a:type = 'grade_smooth')[0]['data'][time.index] AS grade_percent,
    FILTER(RAW_JSON, a -> a:type = 'altitude')[0]['data'][time.index] AS elevation_m,
    
        metadata_filename,
        metadata_last_modified,
    FROM activity_streams_raw
        , LATERAL FLATTEN(INPUT => FILTER(RAW_JSON, a -> a:type = 'time')[0]['data']) time
)

, activity_streams_casted AS (
    SELECT 
    elapsed_time_s::int AS elapsed_time_s,
    is_moving::boolean AS is_moving,
    TO_ARRAY(latlng_array) AS latlng_array,
    heartrate_bpm::float AS heartrate_bpm,
    cadence_rpm::float AS cadence_rpm,
    temperature_c::float AS temperature_c,
    distance_m::float AS distance_m,
    power_watts::float AS power_watts,
    speed_ms::float AS speed_ms,
    grade_percent::float AS grade_percent,
    elevation_m::float AS elevation_m,
    
        metadata_filename,
        metadata_last_modified,
    FROM activity_streams_extracted_and_renamed
)

, activity_streams_with_default_value_imputations AS (
    SELECT 
    elapsed_time_s,
    IFNULL(is_moving, False) as is_moving,
    latlng_array,
    IFF(heartrate_bpm = 0, null, heartrate_bpm) AS heartrate_bpm,
    cadence_rpm,
    temperature_c,
    distance_m,
    power_watts,
    speed_ms,
    grade_percent,
    elevation_m,
    
        metadata_filename,
        metadata_last_modified,
    FROM activity_streams_casted
)

, activity_streams_with_case_when_imputations AS (
    SELECT *
    FROM activity_streams_with_default_value_imputations
)

, activity_streams_with_calculated_fields AS (
SELECT 
    *,
    latlng_array[0]::float AS latitude,
    latlng_array[1]::float AS longitude
FROM activity_streams_with_case_when_imputations
)

, activity_streams_with_keys_and_metadata AS (
    SELECT 
        *,
        metadata_last_modified AS extracted_timestamp,
        regexp_substr(metadata_filename, '\\d+')::int AS activity_id,
        md5(cast(coalesce(cast(activity_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(elapsed_time_s as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as activity_stream_key,
        'strava_api' AS record_source
    FROM activity_streams_with_calculated_fields
)

SELECT 
    -- surrogate keys
    activity_stream_key,
    -- natural keys
    activity_id,
    elapsed_time_s,
    -- dimensions (boolean)
    is_moving,
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
    extracted_timestamp,
    record_source
FROM activity_streams_with_keys_and_metadata