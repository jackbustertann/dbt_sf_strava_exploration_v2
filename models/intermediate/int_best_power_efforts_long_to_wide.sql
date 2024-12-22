-- best power efforts, pivoted from wide to long

{% set effort_durations = [15, 60, 300, 1200, 3600] %}

WITH best_power_efforts_long AS (
    SELECT activity_key, effort_duration_s, average_power_watts
    FROM {{ ref("int_best_power_efforts") }}
)

SELECT
    best_power_effort_15s.activity_key,
{% for effort_duration in effort_durations %}
    best_power_effort_{{ effort_duration }}s_watts{% if not loop.last %},{% endif %}
{% endfor %}
{% for effort_duration in effort_durations %}
    {% if loop.first %}
    FROM (
        SELECT 
            activity_key, 
            average_power_watts AS best_power_effort_{{ effort_duration }}s_watts
        FROM best_power_efforts_long
        WHERE effort_duration_s = {{ effort_duration }}
    ) best_power_effort_{{ effort_duration }}s
    {% endif %}
    {% if not loop.first %}
    LEFT JOIN (
        SELECT 
            activity_key, 
            average_power_watts AS best_power_effort_{{ effort_duration }}s_watts
        FROM best_power_efforts_long
        WHERE effort_duration_s = {{ effort_duration }}
    ) best_power_effort_{{ effort_duration }}s
        ON best_power_effort_15s.activity_key = best_power_effort_{{ effort_duration }}s.activity_key
    {% endif %}
{% endfor %}