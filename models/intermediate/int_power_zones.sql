-- power zones (cycling only)

-- methodology
-- - determine power zones based on ftp estimate, for each date range

-- caveats

-- tests
-- uniqueness: power zone key (start date, zone number)
-- nullability: N/A
-- allowed values:
-- - zone number between 1 and 6
-- referential integrity: N/A

-- future considerations
-- - explode zone bounds into one row per day for better joins + more robust primary key

WITH ftp_changes AS (
    SELECT *
    FROM {{ ref("int_ftp_changes") }}
),

ftp_changes_with_date_ranges AS (
    SELECT
        ftp_key,
        activity_date AS start_date,
        LEAD(activity_date, 1, CONVERT_TIMEZONE('UTC', current_timestamp)::date) OVER(ORDER BY activity_date) AS end_date,
        ftp_watts,
    FROM ftp_changes
),

{% set lower_bounds = [0, 0.6, 0.76, 0.9, 1.05, 1.19] %}
{% set upper_bounds = [0.6, 0.76, 0.9, 1.05, 1.19, -1] %}

power_zones AS (
{% for i in range(lower_bounds|length) %}
    SELECT 
        ftp_key,
        start_date,
        end_date,
        {{ i+1 }} AS zone_number,
        ROUND({{ lower_bounds[i] }} * ftp_watts, 0) AS lower_bound,
        CASE 
            WHEN {{ upper_bounds[i] }} > 0 THEN ROUND({{ upper_bounds[i] }} * ftp_watts, 0)
            ELSE -1
        END AS upper_bound
    FROM ftp_changes_with_date_ranges
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
)

SELECT 
    -- surrogate keys
    {{ dbt_utils.generate_surrogate_key(['start_date', 'zone_number']) }} as power_zone_key,
    ftp_key,
    -- dates
    start_date::date AS start_date,
    end_date::date AS end_date,
    -- dimensions
    zone_number::int AS zone_number,
    -- measures
    lower_bound::int AS lower_bound,
    upper_bound::int AS upper_bound,
    -- technical meta-data
    CONVERT_TIMEZONE('UTC', DATE_TRUNC('second', current_timestamp)) AS loaded_timestamp_utc
FROM power_zones

