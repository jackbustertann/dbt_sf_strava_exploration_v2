-- best power efforts (cycling only)

-- methodology
-- - filter cycling activities, with power data
-- - fill in gaps of activity streams with null power for each activity
-- - calculate moving averages of power over each effort duration for each activity
-- - get best average power for each effort duration for each activity

-- caveats
-- - moving window functions in snowflake can only process a maximum of 1000 rows
-- -> this issue is addressed by breaking down larger effort durations into 5s windows to reduce rows
-- -> this results in an approximation of best effort with 5s confidence interval
-- -> note: best efforts with durations greater than 1000s will have upper and lower bounds divisible by 5
-- - some activity streams do not have 100% coverage of power
-- -> this issue is addressed by setting power to zero for missing streams
-- -> this results in under-estimation of average power for efforts with missing streams
-- -> note: an effort must have atleast 90% coverage to be eligible as a best effort

-- tests
-- uniqueness: best effort key
-- nullability: average power
-- allowed values:
-- - effort duration in [15, 60, 300, 1200, 3600]
-- - effort coverage > 90%
-- - end time >= effort duration
-- referential integrity: activity key -> activities

{% set effort_durations = [15, 60, 300, 1200, 3600] %}
{% set min_effort_coverage = 0.9 %}

WITH activities AS (
    SELECT activity_key, elapsed_time_s
    FROM {{ ref('stg_strava__activities') }}
    WHERE sport = 'ride'
        AND has_power
),

activity_streams AS (
    SELECT activity_key, elapsed_time_s, power_watts
    FROM {{ ref("stg_strava__activity_streams") }}
    WHERE activity_key IN (
        SELECT activity_key
        FROM activities
    )
),

elapsed_time_filled AS (
    SELECT 
        activities.activity_key,
        CAST(time.value AS INT) AS elapsed_time_s
    FROM activities, 
        LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, activities.elapsed_time_s)) time
),

activity_streams_with_full_coverage AS (
    SELECT
        elapsed_time_filled.activity_key,
        elapsed_time_filled.elapsed_time_s,
        activity_streams.power_watts,
        CASE 
            WHEN activity_streams.elapsed_time_s IS NULL THEN false
            ELSE true 
        END AS is_recorded
    FROM elapsed_time_filled
    LEFT JOIN activity_streams
    ON elapsed_time_filled.activity_key = activity_streams.activity_key
        AND elapsed_time_filled.elapsed_time_s = activity_streams.elapsed_time_s
),

activity_efforts AS (
    {% for effort_duration in effort_durations %}

    {% if effort_duration > 1000 %}

    {% set effort_duration_over_five = (effort_duration / 5)|int %}
    {% set effort_duration_over_five_minus_one = (effort_duration_over_five - 1)|int %}

    (
        SELECT 
            activity_key,
            effort_duration,
            (end_time - ({{ effort_duration_over_five }} * 5)) AS start_time,
            end_time,
            SUM(effort_coverage_5s) OVER(PARTITION BY activity_key ORDER BY end_time ROWS BETWEEN {{ effort_duration_over_five_minus_one }} PRECEDING AND CURRENT ROW) AS effort_coverage,
            AVG(IFNULL(average_power_watts_5s, 0)) OVER(PARTITION BY activity_key ORDER BY end_time ROWS BETWEEN {{ effort_duration_over_five_minus_one }} PRECEDING AND CURRENT ROW) AS average_power_watts
        FROM (
            SELECT 
                activity_key,
                {{ effort_duration }} AS effort_duration,
                elapsed_time_s - 4 AS start_time,
                elapsed_time_s AS end_time,
                SUM(CASE WHEN is_recorded THEN 1 ELSE 0 END) OVER(PARTITION BY activity_key ORDER BY elapsed_time_s ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS effort_coverage_5s,
                AVG(IFNULL(power_watts, 0)) OVER(PARTITION BY activity_key ORDER BY elapsed_time_s ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS average_power_watts_5s
            FROM activity_streams_with_full_coverage
        ) AS activity_efforts_5s
        WHERE FLOOR(end_time/5) = end_time/5
    )

    {% elif effort_duration > 0 %}

    {% set effort_duration_minus_one = effort_duration - 1 %}

    (
        SELECT 
            activity_key,
            {{ effort_duration }} AS effort_duration,
            elapsed_time_s - {{ effort_duration_minus_one }} AS start_time,
            elapsed_time_s AS end_time,
            SUM(CASE WHEN is_recorded THEN 1 ELSE 0 END) OVER(PARTITION BY activity_key ORDER BY elapsed_time_s ROWS BETWEEN {{ effort_duration_minus_one }} PRECEDING AND CURRENT ROW) AS effort_coverage,
            AVG(IFNULL(power_watts, 0)) OVER(PARTITION BY activity_key ORDER BY elapsed_time_s ROWS BETWEEN {{ effort_duration_minus_one }} PRECEDING AND CURRENT ROW) AS average_power_watts
        FROM activity_streams_with_full_coverage
    
    )

    {% endif %}

        {% if not loop.last %}UNION ALL{% endif %}
    
    {% endfor %}
),

best_activity_efforts AS (
    SELECT *
    FROM activity_efforts
    WHERE end_time >= effort_duration
        AND effort_coverage / effort_duration >= {{ min_effort_coverage }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY activity_key, effort_duration ORDER BY average_power_watts desc, start_time) = 1
)

SELECT
    -- surrogate keys
    {{ dbt_utils.generate_surrogate_key(['activity_key', 'effort_duration']) }} as best_effort_key,
    activity_key::varchar AS activity_key,
    -- dimensions
    effort_duration::int AS effort_duration_s,
    -- measures
    start_time::int AS start_time_s,
    end_time::int AS end_time_s,
    effort_coverage::int AS effort_coverage_s,
    average_power_watts::float AS average_power_watts,
    -- technical meta-data
    CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc
FROM best_activity_efforts
ORDER BY activity_key, effort_duration
