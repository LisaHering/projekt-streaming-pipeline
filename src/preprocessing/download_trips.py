"""Downloads NYC Taxi dataset for January 2022 from BigQuery as CSV file."""

import os

import pandas_gbq
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
# Override settings via environment variables

DATA_DIR = os.getenv("DATA_DIR", "data")
# Free Google Cloud project used to run the query
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
TABLE = "bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022"
COLUMNS = """pickup_datetime, dropoff_datetime, passenger_count,
    trip_distance, total_amount, pickup_location_id, dropoff_location_id"""
WEEKS = [
    (1, "2022-01-01", "2022-01-07"),
    (2, "2022-01-08", "2022-01-14"),
    (3, "2022-01-15", "2022-01-21"),
    (4, "2022-01-22", "2022-01-28"),
    (5, "2022-01-29", "2022-01-31"),
]


def download_week(number, start_date, end_date):
    """Download a week of taxi trips & save as CSV."""
    query = f"""
        SELECT {COLUMNS}
        FROM `{TABLE}`
        WHERE pickup_datetime >= '{start_date} 00:00:00'
        AND pickup_datetime <= '{end_date} 23:59:59'
        ORDER BY pickup_datetime ASC
    """
    trips = pandas_gbq.read_gbq(
        query, dialect='standard', project_id=GCP_PROJECT_ID)
    file_name = f"nyc_taxi_2022_01_woche_{number}.csv"
    output_path = os.path.join(DATA_DIR, file_name)
    trips.to_csv(output_path, index=False)
    print(f"Successfully saved at '{output_path}', rows: {len(trips):,}")


def main():
    """Download all weeks of January 2022 taxi trips."""
    os.makedirs(DATA_DIR, exist_ok=True)
    for number, start_date, end_date in WEEKS:
        download_week(number, start_date, end_date)


if __name__ == "__main__":
    main()
