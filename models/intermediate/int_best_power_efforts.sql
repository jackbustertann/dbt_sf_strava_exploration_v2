-- steps

-- write pro-type query for calculating best effort for one activity [x]
-- validate best effort value against strava [x]
-- convert query to macro, with following parameters: [x]
---- activity id [x]
---- best effort duration [x]
---- best effort column [x]
-- write pro-type query for calculating best efforts for multiple activities [x]
-- validate best effort values against strava [x]
-- convert query to macro, with following parameters: [x]
---- best effort durations [x]
---- best effort column [x]
-- create if block for effort durations > 1000 [x]
-- add comments / documentation 
-- add materialisation for dev, staging and prod
-- write data quality tests

{{
    config(
        materialized='view' if target.name == 'dev' else 'incremental',
        unique_key='best_effort_key',
        on_schema_change='fail'
    )
}}

{% set measure_col = 'power_watts' %}
{% set effort_durations = [15, 60, 300, 1200, 3600] %}
{% set activity_key_col = 'activity_key' %}
{% set elapsed_time_col = 'elapsed_time_s' %}

WITH activities AS (
    SELECT {{ activity_key_col }}, {{ elapsed_time_col }}
    FROM {{ ref('stg_strava__activities') }}
    {% if target.name == 'dev' %}
    where TO_DATE(loaded_timestamp_utc) >= dateadd('day', -7, current_date)
    {% elif is_incremental() %}
    WHERE loaded_timestamp_utc > (
        SELECT MAX(loaded_timestamp_utc)
        FROM {{ this }}
    )
    {% endif %}
        AND sport = 'ride'
        AND has_power
),

activity_streams AS (
    SELECT {{ activity_key_col }}, {{ elapsed_time_col }}, {{ measure_col }}
    FROM {{ ref("stg_strava__activity_streams") }}
    WHERE activity_key IN (
        SELECT activity_key
        FROM activities
    )
),

elapsed_time_filled AS (
    SELECT 
        activities.{{ activity_key_col }},
        CAST(time.value AS INT) AS {{ elapsed_time_col }}
    FROM activities, 
        LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, activities.{{ elapsed_time_col }})) time
),

activity_streams_with_full_coverage AS (
    SELECT
        elapsed_time_filled.{{ activity_key_col }},
        elapsed_time_filled.{{ elapsed_time_col }},
        activity_streams.{{ measure_col }},
        CASE 
            WHEN activity_streams.{{ elapsed_time_col }} IS NULL THEN false
            ELSE true 
        END AS is_recorded
    FROM elapsed_time_filled
    LEFT JOIN activity_streams
    ON elapsed_time_filled.{{ activity_key_col }} = activity_streams.{{ activity_key_col }}
        AND elapsed_time_filled.{{ elapsed_time_col }} = activity_streams.{{ elapsed_time_col }}
),

activity_efforts AS (
    {% for effort_duration in effort_durations %}

    {% if effort_duration > 1000 %}

    {% set effort_duration_over_five = (effort_duration / 5)|int %}
    {% set effort_duration_over_five_minus_one = (effort_duration_over_five - 1)|int %}

    (
        SELECT 
            {{ activity_key_col }},
            effort_duration,
            (end_time - ({{ effort_duration_over_five }} * 5)) AS start_time,
            end_time,
            {{ measure_col }},
            SUM(effort_coverage_5s) OVER(ORDER BY end_time ROWS BETWEEN {{ effort_duration_over_five_minus_one }} PRECEDING AND CURRENT ROW) AS effort_coverage,
            AVG(IFNULL(avg_{{ measure_col }}_5s, 0)) OVER(ORDER BY end_time ROWS BETWEEN {{ effort_duration_over_five_minus_one }} PRECEDING AND CURRENT ROW) AS avg_{{ measure_col }}
        FROM (
            SELECT 
                {{ activity_key_col }},
                '{{ effort_duration }}' AS effort_duration,
                elapsed_time_s - 4 AS start_time,
                elapsed_time_s AS end_time,
                {{ measure_col }},
                SUM(CASE WHEN is_recorded THEN 1 ELSE 0 END) OVER(PARTITION BY {{ activity_key_col }} ORDER BY elapsed_time_s ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS effort_coverage_5s,
                AVG(IFNULL({{ measure_col }}, 0)) OVER(PARTITION BY {{ activity_key_col }} ORDER BY elapsed_time_s ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS avg_{{ measure_col }}_5s
            FROM activity_streams_with_full_coverage
            WHERE FLOOR(elapsed_time_s/5) = elapsed_time_s/5
        ) AS activity_efforts_5s
    )

    {% elif effort_duration > 0 %}

    {% set effort_duration_minus_one = effort_duration - 1 %}

    (
        SELECT 
            {{ activity_key_col }},
            '{{ effort_duration }}' AS effort_duration,
            {{ elapsed_time_col }} - {{ effort_duration_minus_one }} AS start_time,
            {{ elapsed_time_col }} AS end_time,
            {{ measure_col }},
            SUM(CASE WHEN is_recorded THEN 1 ELSE 0 END) OVER(PARTITION BY {{ activity_key_col }} ORDER BY {{ elapsed_time_col }} ROWS BETWEEN {{ effort_duration_minus_one }} PRECEDING AND CURRENT ROW) AS effort_coverage,
            AVG(IFNULL({{ measure_col }}, 0)) OVER(PARTITION BY {{ activity_key_col }} ORDER BY {{ elapsed_time_col }} ROWS BETWEEN {{ effort_duration_minus_one }} PRECEDING AND CURRENT ROW) AS avg_{{ measure_col }}
        FROM activity_streams_with_full_coverage
    
    )

    {% endif %}

        {% if not loop.last %}UNION ALL{% endif %}
    
    {% endfor %}
),

activity_efforts_ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY {{ activity_key_col }}, effort_duration ORDER BY avg_{{ measure_col }} desc, start_time) as effort_rank
    FROM activity_efforts
    WHERE start_time >= 0
    ORDER BY {{ activity_key_col }}, effort_duration, effort_rank
)

SELECT
    -- surrogate keys
    {{ dbt_utils.generate_surrogate_key(['activity_key', 'effort_duration']) }} as best_effort_key,
    activity_key,
    -- dimensions
    effort_duration,
    -- measures
    start_time,
    end_time,
    effort_coverage,
    {{ measure_col }},
    avg_{{ measure_col }},
    -- technical meta-data
    CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc
FROM activity_efforts_ranked
WHERE effort_rank = 1
