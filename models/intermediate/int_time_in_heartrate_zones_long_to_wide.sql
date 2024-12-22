-- time in heartrate zones, pivoted from long to wide

{% set heartrate_zones = [1, 2, 3, 4, 5] %}

WITH time_in_heartrate_zones_long AS (
    SELECT activity_key, zone_number, moving_time_in_zone_s
    FROM {{ ref("int_time_in_heartrate_zones") }}
)

SELECT
    time_in_heartrate_zone_1.activity_key,
{% for heartrate_zone in heartrate_zones %}
    COALESCE(moving_time_in_zone_{{ heartrate_zone }}_s, 0) AS moving_time_in_zone_{{ heartrate_zone }}_s{% if not loop.last %},{% endif %}
{% endfor %}
{% for heartrate_zone in heartrate_zones %}
    {% if loop.first %}
    FROM (
        SELECT 
            activity_key, 
            moving_time_in_zone_s AS moving_time_in_zone_{{ heartrate_zone }}_s
        FROM time_in_heartrate_zones_long
        WHERE zone_number = {{ heartrate_zone }}
    ) time_in_heartrate_zone_{{ heartrate_zone }}
    {% endif %}
    {% if not loop.first %}
    LEFT JOIN (
        SELECT 
            activity_key, 
            moving_time_in_zone_s AS moving_time_in_zone_{{ heartrate_zone }}_s
        FROM time_in_heartrate_zones_long
        WHERE zone_number = {{ heartrate_zone }}
    ) time_in_heartrate_zone_{{ heartrate_zone }}
        ON time_in_heartrate_zone_1.activity_key = time_in_heartrate_zone_{{ heartrate_zone }}.activity_key
    {% endif %}
{% endfor %}
