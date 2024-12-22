-- best distance efforts (ride), pivoted from long to wide

{% set effort_distances = [8000, 16000, 32000] %}

WITH best_distance_efforts_ride_long AS (
    SELECT activity_key, effort_distance_m, average_speed_adjusted_kmhr AS average_speed_kmhr
    FROM {{ ref("int_best_distance_efforts_ride") }}
)

SELECT
    best_distance_effort_8km.activity_key,
{% for effort_distance_m in effort_distances %}
    {% set effort_distance_km = (effort_distance_m / 1000)|int %}
    best_distance_effort_{{ effort_distance_km }}km_kmhr{% if not loop.last %},{% endif %}
{% endfor %}
{% for effort_distance_m in effort_distances %}
    {% set effort_distance_km = (effort_distance_m / 1000)|int %}
    {% if loop.first %}
    FROM (
        SELECT 
            activity_key, 
            average_speed_kmhr AS best_distance_effort_{{ effort_distance_km }}km_kmhr
        FROM best_distance_efforts_ride_long
        WHERE effort_distance_m = {{ effort_distance_m }}
    ) best_distance_effort_{{ effort_distance_km }}km
    {% endif %}
    {% if not loop.first %}
    LEFT JOIN (
        SELECT 
            activity_key, 
            average_speed_kmhr AS best_distance_effort_{{ effort_distance_km }}km_kmhr
        FROM best_distance_efforts_ride_long
        WHERE effort_distance_m = {{ effort_distance_m }}
    ) best_distance_effort_{{ effort_distance_km }}km
        ON best_distance_effort_8km.activity_key = best_distance_effort_{{ effort_distance_km }}km.activity_key
    {% endif %}
{% endfor %}