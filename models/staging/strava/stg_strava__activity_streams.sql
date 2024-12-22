-- strava activity streams, staged

-- methodology
-- - extract required keys from JSON
-- - rename columns
-- - cast data-types
-- - convert units
-- - null/zero imputations
-- - special case imputations
-- - basic calculated fields (e.g. extract lat/long from array)
-- - add file meta-data (created/extracted/loaded ts, source system)
-- - generate surrogate keys

{{
    config(
        materialized='table' if target.name == 'dev' else 'incremental',
        unique_key='activity_stream_key',
        on_schema_change='fail'
    )
}}

with activity_streams_raw as (
    select *
    from {{ source('strava_api_v3', 'strava__activity_streams') }}
    {% if target.name == 'dev' %}
    where TO_DATE(metadata_last_modified) >= dateadd('day', -28, current_date)
    {% elif is_incremental() %}
    WHERE TO_TIMESTAMP_TZ(metadata_last_modified || '+00:00') > (
        SELECT MAX(loaded_timestamp_utc)
        FROM {{ this }}
    ) -- TODO: move casting of last modified timestamp to raw table
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
    {'json_key': 'velocity_smooth', 'data_type': 'float', 'column_name': 'speed_ms'},
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
        TO_TIMESTAMP_TZ(metadata_last_modified || '+00:00') AS extracted_timestamp_utc,
        CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc,
        regexp_substr(metadata_filename, '\\d+')::int AS activity_id,
        {{ dbt_utils.generate_surrogate_key(['activity_id']) }} as activity_key,
        {{ dbt_utils.generate_surrogate_key(['activity_key', 'elapsed_time_s']) }} as activity_stream_key,
        'strava-api-v3/' || metadata_filename AS record_source
    FROM activity_streams_with_calculated_fields
)

SELECT 
    -- surrogate keys
    activity_stream_key,
    activity_key,
    -- natural keys
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
    speed_ms * 3.6 AS speed_kmhr,
    power_watts,
    -- technical meta-data
    extracted_timestamp_utc,
    loaded_timestamp_utc,
    record_source
FROM activity_streams_with_keys_and_metadata