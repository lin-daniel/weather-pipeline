-- models/stg_weather.sql

WITH raw_source AS (
    -- 1. Identify our source table
    -- (Replace 'weather-pipeline-486804' with YOUR actual Project ID)
    SELECT * FROM `weather-pipeline-486804.weather_data.raw_data`
)

SELECT
    -- 2. Select and Rename Columns
    city_name,

    -- Cast the string timestamp to a real Timestamp object
    CAST(fetched_at AS TIMESTAMP) as extraction_time,

    -- 3. Un-nest the JSON data
    -- Because we used 'autodetect', BigQuery sees 'current_weather' as a Record.
    -- We can access the fields inside it using dot notation.
    current_weather.temperature as temperature_c,
    current_weather.windspeed as wind_speed_kmh,
    current_weather.weathercode as weather_code,

    -- Keep the coordinates just in case
    latitude,
    longitude

FROM raw_source