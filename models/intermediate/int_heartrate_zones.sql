-- heartrate zones (cycling + running)

-- methodology
-- - filter date dimension table for days which fall on birthday and calulate age
-- - estimate max theoretical heartrate based on age
-- - determine heartrate zones based on max heartrate, for each date range

-- caveats
-- - indoor activities lead to a slight over-estimation in ftp vs outdoor activities
-- - ftp is less accurate for periods with small number of race / time trial activities

-- tests
-- uniqueness: heartrate zone key (start date, zone number)
-- nullability: N/A
-- allowed values:
-- - zone number between 1 and 5
-- referential integrity: N/A

-- future considerations
-- - explode zone bounds into one row per day for better joins + more robust primary key
-- - research other methodologies for calculating heartrate zones which consider resting heartrate

{% set dob = '1997-06-19' %}

WITH age_date_changes AS (
    SELECT
        TO_DATE(date_key, 'yyyymmdd') AS start_date,
        LEAST(DATEADD(year, 1, TO_DATE(date_key, 'yyyymmdd')), CONVERT_TIMEZONE('UTC', current_timestamp)::date) AS end_date,
        DATEDIFF(year, TO_DATE('{{ dob }}', 'yyyy-mm-dd'), TO_DATE(date_key, 'yyyymmdd')) AS age_years
    FROM {{ ref("dim_dates") }}
    WHERE TO_VARCHAR(TO_DATE(date_key, 'yyyymmdd'), 'mm-dd') = TO_VARCHAR(TO_DATE('{{ dob }}', 'yyyy-mm-dd'), 'mm-dd')
),

max_heartrate_date_changes AS (
    SELECT 
        start_date,
        end_date,
        ROUND(211 - (0.64 * age_years), 0) AS estimated_max_hr
    FROM age_date_changes
),

{% set lower_bounds = [0, 0.59, 0.78, 0.87, 0.97] %}
{% set upper_bounds = [0.59, 0.78, 0.87, 0.97, -1] %}

heartrate_zones AS (
{% for i in range(lower_bounds|length) %}
    SELECT 
        start_date,
        end_date,
        {{ i+1 }} AS zone_number,
        ROUND({{ lower_bounds[i] }} * estimated_max_hr, 0) AS lower_bound,
        CASE 
            WHEN {{ upper_bounds[i] }} > 0 THEN ROUND({{ upper_bounds[i] }} * estimated_max_hr, 0)
            ELSE -1
        END AS upper_bound
    FROM max_heartrate_date_changes
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
)

SELECT 
    -- surrogate keys
    {{ dbt_utils.generate_surrogate_key(['start_date', 'zone_number']) }} as heartrate_zone_key,
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
FROM heartrate_zones