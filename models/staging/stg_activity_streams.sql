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

{{
    config(
        materialized='incremental',
        unique_key='activity_stream_key'
    )
}}

with activity_streams_raw as (
    select *
    from {{ source('strava_api', 'strava_activity_streams') }}
    {% if is_incremental() %}
    WHERE metadata_last_modified > (
        SELECT MAX(extracted_timestamp)
        FROM {{ this }}
    )
    {% endif %}
)

{% set columns_lst = [
    {'json_key': 'time', 'data_type': 'int', 'column_name': 'elapsed_time_s'},
    {'json_key': 'moving', 'data_type': 'bool', 'column_name': 'is_moving', 'impute_on': 'null', 'default_value': false},
    {'json_key': 'latlng', 'data_type': 'array', 'column_name': 'latlng_array'},
    {'json_key': 'heartrate', 'data_type': 'float', 'column_name': 'heartrate_bpm', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'cadence', 'data_type': 'float', 'column_name': 'cadence_rpm'},
    {'json_key': 'temp', 'data_type': 'float', 'column_name': 'temperature_c'},
    {'json_key': 'distance', 'data_type': 'float', 'column_name': 'distance_m'},
    {'json_key': 'watts', 'data_type': 'float', 'column_name': 'power_watts'},
    {'json_key': 'velocity_smooth', 'data_type': 'float', 'column_name': 'speed_kmhr'},
    {'json_key': 'grade_smooth', 'data_type': 'float', 'column_name': 'grade_percent'},
    {'json_key': 'altitude', 'data_type': 'float', 'column_name': 'elevation_m'}
    ]
%}

, activity_streams_extracted_and_renamed AS (
    SELECT 
    {% for col in columns_lst -%}
        FILTER(RAW_JSON, a -> a:type = '{{ col.json_key }}')[0]['data'][time.index] AS {{ col.column_name }},
    {% endfor %}
        metadata_filename,
        metadata_last_modified,
    FROM activity_streams_raw
        , LATERAL FLATTEN(INPUT => FILTER(RAW_JSON, a -> a:type = 'time')[0]['data']) time
)

, activity_streams_casted AS (
    SELECT 
    {% for col in columns_lst -%}
        {{
            cast_column(
                col.column_name, col.data_type
            )
        }},
    {% endfor %}
        metadata_filename,
        metadata_last_modified,
    FROM activity_streams_extracted_and_renamed
)

, activity_streams_with_default_value_imputations AS (
    SELECT 
    {% for col in columns_lst -%}
        {%- if 'impute_on' in col.keys() -%}
        {{
            impute_column(
                col.column_name, col.impute_on, col.default_value
            )
        }},
        {%- else -%}
        {{ col.column_name }},
        {%- endif %}
    {% endfor %}
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
        {{ dbt_utils.generate_surrogate_key(['activity_id', 'elapsed_time_s']) }} as activity_stream_key,
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
    speed_kmhr,
    power_watts,
    -- technical meta-data
    extracted_timestamp,
    record_source
FROM activity_streams_with_keys_and_metadata