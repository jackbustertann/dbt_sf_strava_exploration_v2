-- best distance efforts (running only)

-- methodology
-- - filter running activities
-- - calculate moving elapsed time and distance bounds over each effort distance for each activity
-- - calculate moving total elapsed time + average speed using elapsed time and distance bounds for each effort distance for each activity
-- - get best average speed for each effort distance for each activity

-- caveats
-- - most activity streams do not have 100% coverage of effort distances
-- -> this issue is addressed by adjusting average speed by the distance coverage %
-- -> this results in an approximation of average speed and elapsed time for each effort distance
-- -> note: an effort must have atleast 95% coverage to be eligible as a best effort

-- tests
-- uniqueness: best distance effort key (activity key, effort distance)
-- nullability: average speed, elapsed time
-- allowed values:
-- - effort duration in [1000, 1600, 3000, 5000, 8000, 10000, 16000, 21100]
-- - effort coverage > 95%
-- - end time >= effort distance
-- referential integrity: N/A

-- future considerations
-- - extend to running activities

{% set effort_distances = [1000, 1600, 3000, 5000, 8000, 10000, 16000, 21100] %}

WITH activities AS (
    SELECT activity_key
    FROM {{ ref('stg_strava__activities') }}
    WHERE sport = 'run'
),

activity_streams AS (
    SELECT activity_key, distance_m, elapsed_time_s
    FROM {{ ref("stg_strava__activity_streams") }}
    WHERE activity_key IN (
        SELECT activity_key
        FROM activities
    )
),

activity_efforts_with_speeds AS (
    {% for effort_distance in effort_distances %}
    (
        SELECT 
            activity_key,
            effort_distance,
            start_time,
            end_time,
            start_distance,
            end_distance,
            end_distance - start_distance AS distance_delta,
            end_time - start_time AS time_delta,
            distance_delta / {{ effort_distance }} AS adjustment_factor,
            time_delta / adjustment_factor AS time_delta_adjusted,
            (distance_delta / time_delta) * 3.6 AS average_speed_kmhr,
            (distance_delta / time_delta_adjusted) * 3.6 AS average_speed_adjusted_kmhr
        FROM (
            SELECT 
                activities.activity_key,
                {{ effort_distance }} AS effort_distance,
                MIN(activity_streams.distance_m) OVER(PARTITION BY activity_streams.activity_key ORDER BY activity_streams.distance_m RANGE BETWEEN {{ effort_distance }} PRECEDING AND CURRENT ROW) AS start_distance,
                MAX(activity_streams.distance_m) OVER(PARTITION BY activity_streams.activity_key ORDER BY activity_streams.distance_m RANGE BETWEEN {{ effort_distance }} PRECEDING AND CURRENT ROW) AS end_distance,
                MIN(activity_streams.elapsed_time_s) OVER(PARTITION BY activity_streams.activity_key ORDER BY activity_streams.distance_m RANGE BETWEEN {{ effort_distance }} PRECEDING AND CURRENT ROW) AS start_time,
                MAX(activity_streams.elapsed_time_s) OVER(PARTITION BY activity_streams.activity_key ORDER BY activity_streams.distance_m RANGE BETWEEN {{ effort_distance }} PRECEDING AND CURRENT ROW) AS end_time
            FROM activities
            JOIN activity_streams
                ON activities.activity_key = activity_streams.activity_key
        ) activity_efforts_with_time_and_distance_bounds
        WHERE distance_delta > ({{ effort_distance }} * 0.95)
        QUALIFY ROW_NUMBER() OVER(PARTITION BY activity_key, start_time ORDER BY end_time DESC) = 1
    ) 

    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

SELECT 
    -- surrogate keys
    {{ dbt_utils.generate_surrogate_key(['activity_key', 'effort_distance']) }} as best_distance_effort_key,
    activity_key::varchar AS activity_key,
    -- dimensions
    effort_distance::int AS effort_distance_m,
    -- measures
    start_time::int AS start_time_s,
    end_time::int AS end_time_s,
    distance_delta::float AS effort_coverage_m,
    time_delta::float AS elapsed_time_s,
    average_speed_kmhr::float AS average_speed_kmhr,
    adjustment_factor::float AS adjustment_factor,
    time_delta_adjusted::float AS elapsed_time_adjusted_s,
    average_speed_adjusted_kmhr::float AS average_speed_adjusted_kmhr,
    -- technical meta-data
    CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc
FROM activity_efforts_with_speeds
QUALIFY ROW_NUMBER() OVER(PARTITION BY activity_key, effort_distance ORDER BY average_speed_kmhr DESC, start_time_s) = 1