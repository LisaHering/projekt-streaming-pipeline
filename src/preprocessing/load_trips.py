"""Load weekly trips table from CSV into PostgreSQL."""

import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
# Override settings via environment variables

DATA_DIR = os.getenv("DATA_DIR", "data")
TRIP_FILES = [
    "nyc_taxi_2022_01_woche_1.csv",
    "nyc_taxi_2022_01_woche_2.csv",
    "nyc_taxi_2022_01_woche_3.csv",
    "nyc_taxi_2022_01_woche_4.csv",
    "nyc_taxi_2022_01_woche_5.csv",
]
CHUNK_SIZE = 50000
COMMIT_EVERY = 50000
MAX_PASSENGERS = 8


def connect_db():
    """Open new PostgreSQL connection with configurations."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "taxi"),
        user=os.getenv("DB_USER", "taxi_user"),
        password=os.environ["DB_PASSWORD"],
    )


# --- VALIDATION ---
# Trips that fail a rule are counted and skipped, not loaded

def rejection_reason(row, pickup, dropoff, valid_zones):
    """Return reason why a trip row is rejected, None if it is valid."""
    required = [pickup, dropoff, row["passenger_count"], row["trip_distance"],
                row["total_amount"], row["pickup_location_id"],
                row["dropoff_location_id"]]
    if any(pd.isna(value) for value in required):
        return "missing_values"
    if pickup >= dropoff:
        return "time_order"
    if row["trip_distance"] <= 0:
        return "distance"
    if row["total_amount"] <= 0:
        return "price"
    if not (1 <= row["passenger_count"] <= MAX_PASSENGERS):
        return "passengers"
    if int(row["pickup_location_id"]) not in valid_zones \
            or int(row["dropoff_location_id"]) not in valid_zones:
        return "invalid_zones"
    return None


def insert_trip(cursor, row, pickup, dropoff):
    """Insert a single trip row into raw_trips table."""
    cursor.execute(
        """INSERT INTO raw_trips
            (pickup_datetime, dropoff_datetime, passenger_count,
            pickup_zone, dropoff_zone, trip_distance, total_amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (pickup, dropoff, int(row["passenger_count"]),
         int(row["pickup_location_id"]), int(row["dropoff_location_id"]),
         row["trip_distance"], row["total_amount"])
    )


def load_trips(connection, valid_zones):
    """Validate and insert all trips; return counters for summary."""
    cursor = connection.cursor()
    processed = 0
    inserted = 0
    rejected = {}
    for file_name in TRIP_FILES:
        csv_path = os.path.join(DATA_DIR, file_name)
        print(f"loading {csv_path} ... ")
        for chunk in pd.read_csv(csv_path, chunksize=CHUNK_SIZE):
            for _, row in chunk.iterrows():
                processed += 1
                pickup = pd.to_datetime(
                    row["pickup_datetime"], errors="coerce")
                dropoff = pd.to_datetime(
                    row["dropoff_datetime"], errors="coerce")
                reason = rejection_reason(row, pickup, dropoff, valid_zones)
                if reason is not None:
                    rejected[reason] = rejected.get(reason, 0) + 1
                    continue
                insert_trip(cursor, row, pickup, dropoff)
                inserted += 1
                if inserted % COMMIT_EVERY == 0:
                    connection.commit()
    connection.commit()
    cursor.close()
    return processed, inserted, rejected


def main():
    """Load trips once; refuse to run if raw_trips table is not empty."""
    connection = connect_db()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM raw_trips")
        if cursor.fetchone()[0] > 0:
            print("Table 'raw_trips' is not empty. Refusing to load trips. "
                  "Run 'TRUNCATE raw_trips;' first to reload.")
            return
        cursor.execute("SELECT location_id FROM zones")
        valid_zones = {row[0] for row in cursor.fetchall()}
        cursor.close()
        processed, inserted, rejected = load_trips(connection, valid_zones)
    finally:
        connection.close()
    print(f"Processed {processed} rows, "
          f"inserted {inserted} rows into the raw_trips table.")
    print(f"Rejected rows: {sum(rejected.values())}")
    for reason, count in rejected.items():
        print(f"  {reason}: {count}")


if __name__ == "__main__":
    main()
