import requests
import datetime
import sys
from google.cloud import bigquery

# CONFIGURATION
TABLE_ID = "weather-pipeline-486804.weather_data.raw_data"

cities = [
    {"name": "London", "lat": 51.5074, "lon": -0.1278},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060}
]


def run_extraction():
    # Initialize Client (Docker will use the ENV variable automatically)
    try:
        client = bigquery.Client()
    except Exception as e:
        print(f"FATAL: Could not initialize BigQuery Client. Check GOOGLE_APPLICATION_CREDENTIALS. Error: {e}")
        sys.exit(1)

    rows_to_insert = []
    print("--- Starting Extraction ---")

    # 1. LOOP & EXTRACT
    for city in cities:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "current_weather": "true"
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            # 2. TRANSFORM
            data["fetched_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            data["city_name"] = city["name"]

            rows_to_insert.append(data)
            print(f"✓ Fetched data for {city['name']}")

        except requests.exceptions.RequestException as e:
            print(f"✗ Failed to fetch {city['name']}: {e}")

    # 3. LOAD (Batch Load)
    if rows_to_insert:
        print(f"--- Loading {len(rows_to_insert)} rows to BigQuery ---")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
            autodetect=True,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )

        try:
            job = client.load_table_from_json(rows_to_insert, TABLE_ID, job_config=job_config)
            job.result()  # Waits for the job to complete
            print(f"Success! Loaded {job.output_rows} rows into {TABLE_ID}.")

        except Exception as e:
            print(f"FATAL: BigQuery Load Failed: {e}")
            if hasattr(e, 'errors'):
                print(e.errors)
            sys.exit(1)  # Signal failure to Docker
    else:
        print("Warning: No data to load.")
        # Optional: sys.exit(1) here if you consider 0 rows a failure


if __name__ == "__main__":
    run_extraction()