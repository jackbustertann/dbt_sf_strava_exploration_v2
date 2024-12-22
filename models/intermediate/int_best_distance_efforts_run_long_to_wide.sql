-- best distance efforts (run), pivoted from long to wide

{% set effort_distances = [1000, 1600, 3000, 5000, 8000, 10000, 16000, 21100] %}

WITH best_distance_efforts_ride_long AS (
    SELECT activity_key, effort_distance_m, CEIL(elapsed_time_adjusted_s) AS elapsed_time_s
    FROM {{ ref("int_best_distance_efforts_run") }}
)

SELECT
    best_distance_effort_1000m.activity_key,
{% for effort_distance_m in effort_distances %}
    best_distance_effort_{{ effort_distance_m }}m_s{% if not loop.last %},{% endif %}
{% endfor %}
{% for effort_distance_m in effort_distances %}
    {% if loop.first %}
    FROM (
        SELECT 
            activity_key, 
            elapsed_time_s AS best_distance_effort_{{ effort_distance_m }}m_s
        FROM best_distance_efforts_ride_long
        WHERE effort_distance_m = {{ effort_distance_m }}
    ) best_distance_effort_{{ effort_distance_m }}m
    {% endif %}
    {% if not loop.first %}
    LEFT JOIN (
        SELECT 
            activity_key, 
            elapsed_time_s AS best_distance_effort_{{ effort_distance_m }}m_s
        FROM best_distance_efforts_ride_long
        WHERE effort_distance_m = {{ effort_distance_m }}
    ) best_distance_effort_{{ effort_distance_m }}m
        ON best_distance_effort_1000m.activity_key = best_distance_effort_{{ effort_distance_m }}m.activity_key
    {% endif %}
{% endfor %}