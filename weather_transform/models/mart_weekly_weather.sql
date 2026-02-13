-- models/mart_weekly_weather.sql

WITH source_data AS (
    SELECT * FROM {{ ref('stg_weather') }}
)

SELECT
    city_name,
    extraction_time,
    temperature_c,
    wind_speed_kmh,

    -- Window Function: Calculate average temp over the last 7 rows (days)
    AVG(temperature_c) OVER (
        PARTITION BY city_name
        ORDER BY extraction_time
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as avg_temp_7_days,

    -- Business Logic: "Is it nice outside?"
    CASE
        WHEN temperature_c > 25 THEN 'Hot'
        WHEN temperature_c < 10 THEN 'Cold'
        ELSE 'Comfortable'
    END as comfort_level

FROM source_data