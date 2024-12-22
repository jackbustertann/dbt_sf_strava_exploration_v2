-- strava activities, staged

-- methodology
-- - extract required keys from JSON
-- - rename columns
-- - cast data-types
-- - convert units
-- - null/zero default value imputations (e.g. zero power)
-- - case when imputations (e.g. in-accurate heartrate/power readings, manual uploads)
-- - basic calculated fields (e.g. sport, is_indoor, is_race)
-- - add file meta-data (created/extracted/loaded ts, source system)
-- - generate surrogate keys

{{
    config(
        materialized='table' if target.name == 'dev' else 'incremental',
        unique_key='activity_key',
        on_schema_change='fail'
    )
}}

with activities_raw as (
    select *
    from {{ source('strava_api_v3', 'strava__activities') }}
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
    {'json_key': 'id', 'data_type': 'int', 'column_name': 'activity_id', 'impute_on': 'null', 'default_value': -1},
    {'json_key': 'name', 'data_type': 'str', 'column_name': 'activity_name'},
    {'json_key': 'type', 'data_type': 'str', 'column_name': 'activity_type'},
    {'json_key': 'has_heartrate', 'data_type': 'bool', 'column_name': 'has_heartrate', 'impute_on': 'null', 'default_value': false},
    {'json_key': 'device_watts', 'data_type': 'bool', 'column_name': 'has_power', 'impute_on': 'null', 'default_value': false},
    {'json_key': 'manual', 'data_type': 'bool', 'column_name': 'is_manual', 'impute_on': 'null', 'default_value': false},
    {'json_key': 'distance', 'data_type': 'float', 'column_name': 'distance_m', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'elev_low', 'data_type': 'float', 'column_name': 'min_elevation_m', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'elev_high', 'data_type': 'float', 'column_name': 'max_elevation_m', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'total_elevation_gain', 'data_type': 'float', 'column_name': 'elevation_gain_m', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'moving_time', 'data_type': 'float', 'column_name': 'moving_time_s', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'elapsed_time', 'data_type': 'float', 'column_name': 'elapsed_time_s', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'average_speed', 'data_type': 'float', 'column_name': 'average_speed_ms', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'max_speed', 'data_type': 'float', 'column_name': 'max_speed_ms', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'average_cadence', 'data_type': 'float', 'column_name': 'average_cadence_rpm', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'average_temp', 'data_type': 'float', 'column_name': 'average_tempature_c', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'kilojoules', 'data_type': 'float', 'column_name': 'calories_kj', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'average_heartrate', 'data_type': 'float', 'column_name': 'average_heartrate_bpm', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'max_heartrate', 'data_type': 'float', 'column_name': 'max_heartrate_bpm', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'suffer_score', 'data_type': 'float', 'column_name': 'suffer_score', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'average_watts', 'data_type': 'float', 'column_name': 'average_power_watts', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'weighted_average_watts', 'data_type': 'float', 'column_name': 'normalised_power_watts', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'max_watts', 'data_type': 'float', 'column_name': 'max_power_watts', 'impute_on': 'zero', 'default_value': 'null'},
    {'json_key': 'start_date_local', 'data_type': 'ntz', 'column_name': 'activity_start_timestamp_ntz', 'impute_on': 'null', 'default_value': 'null_ntz'}
    ]
%}
, activities_extracted_and_renamed AS (
    SELECT 
    {% for col in columns_lst -%}
        get(VALUE, '{{ col.json_key }}') AS {{ col.column_name }},
    {% endfor %}
        metadata_filename,
        metadata_last_modified,
    FROM activities_raw, LATERAL FLATTEN(INPUT => RAW_JSON)
)

, activities_casted AS (
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
    FROM activities_extracted_and_renamed
)

, activities_with_default_value_imputations AS (
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
            WHEN TO_DATE(activity_start_timestamp_ntz) <= TO_DATE('2022-10-23', 'YYYY-MM-DD') THEN false
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
        TO_TIMESTAMP_TZ(metadata_last_modified || '+00:00') AS extracted_timestamp_utc,
        CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc,
        {{ dbt_utils.generate_surrogate_key(['activity_id']) }} as activity_key,
        TO_VARCHAR(activity_start_timestamp_ntz, 'yyyymmdd') as start_date_key,
        'strava-api-v3/' || metadata_filename AS record_source
    FROM activities_with_calculated_fields
)

SELECT 
    -- surrogate keys
    activity_key,
    start_date_key,
    -- natural keys
    activity_id,
    -- dates
    activity_start_timestamp_ntz,
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
    average_speed_ms * 3.6 AS average_speed_kmhr,
    max_speed_ms * 3.6 AS max_speed_kmhr,
    average_power_watts,
    max_power_watts,
    normalised_power_watts,
    -- technical meta-data
    extracted_timestamp_utc,
    loaded_timestamp_utc,
    record_source
FROM activities_with_keys_and_metadata
