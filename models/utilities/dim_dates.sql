{{
    config(
        materialized='view' if target.name == 'dev' else 'table',
    )
}}

{% set start_date_str = '2020-01-01' %}

WITH dates AS (
    SELECT 
        DATEADD(day, date_array_index.value, '{{ start_date_str }}'::date) AS date
    FROM LATERAL FLATTEN(
        INPUT => ARRAY_GENERATE_RANGE(
            0, 
            DATEDIFF(day, '{{ start_date_str }}'::date, CONVERT_TIMEZONE('UTC', current_timestamp)::date) + 1
        )
    ) date_array_index
)

SELECT 
    TO_VARCHAR(date, 'yyyymmdd') AS date_key,
    TO_VARCHAR(date, 'yyyy-mm-dd') AS date_day,
    TO_VARCHAR(DATE_TRUNC(week, date), 'yyyy-mm-dd') AS date_week,
    TO_VARCHAR(date, 'yyyy-mm') AS date_month,
    TO_VARCHAR(date, 'yyyy') || '-Q' || QUARTER(date) AS date_quarter,
    TO_VARCHAR(date, 'yyyy') AS date_year,
    decode(
        extract(dayofweek from date),
        1, 'Monday',
        2, 'Tuesday',
        3, 'Wednesday',
        4, 'Thursday',
        5, 'Friday',
        6, 'Saturday',
        0, 'Sunday'
    ) AS date_day_name,
    date_day_name IN ('Saturday', 'Sunday') AS date_is_weekend
FROM dates