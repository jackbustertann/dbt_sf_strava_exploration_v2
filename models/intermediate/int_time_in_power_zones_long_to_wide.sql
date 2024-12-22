-- time in power zones, pivoted from long to wide

{% set power_zones = [1, 2, 3, 4, 5, 6] %}

WITH time_in_power_zones_long AS (
    SELECT activity_key, zone_number, moving_time_in_zone_s
    FROM {{ ref("int_time_in_power_zones") }}
)

SELECT
    time_in_power_zone_1.activity_key,
{% for power_zone in power_zones %}
    COALESCE(moving_time_in_zone_{{ power_zone }}_s, 0) AS moving_time_in_zone_{{ power_zone }}_s{% if not loop.last %},{% endif %}
{% endfor %}
{% for power_zone in power_zones %}
    {% if loop.first %}
    FROM (
        SELECT 
            activity_key, 
            moving_time_in_zone_s AS moving_time_in_zone_{{ power_zone }}_s
        FROM time_in_power_zones_long
        WHERE zone_number = {{ power_zone }}
    ) time_in_power_zone_{{ power_zone }}
    {% endif %}
    {% if not loop.first %}
    LEFT JOIN (
        SELECT 
            activity_key, 
            moving_time_in_zone_s AS moving_time_in_zone_{{ power_zone }}_s
        FROM time_in_power_zones_long
        WHERE zone_number = {{ power_zone }}
    ) time_in_power_zone_{{ power_zone }}
        ON time_in_power_zone_1.activity_key = time_in_power_zone_{{ power_zone }}.activity_key
    {% endif %}
{% endfor %}
