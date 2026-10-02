"""Load taxi zone lookup table from CSV into PostgreSQL."""

import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Override settings via environment variables
DATA_DIR = os.getenv("DATA_DIR", "data")
ZONES_FILE = os.getenv("ZONES_FILE", "taxi_zone_lookup.csv")


def connect_db():
    """Open new PostgreSQL connection with configurations."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "taxi"),
        user=os.getenv("DB_USER", "taxi_user"),
        password=os.environ["DB_PASSWORD"],
    )


def read_zones():
    """Read zones from CSV file and rename columns to match zones table."""
    csv_path = os.path.join(DATA_DIR, ZONES_FILE)
    zones = pd.read_csv(csv_path, keep_default_na=False)
    return zones.rename(columns={
        "LocationID": "location_id",
        "Borough": "borough",
        "Zone": "zone",
    })


def insert_zones(cursor, zones):
    """Insert zones into the PostgreSQL database."""
    for _, row in zones.iterrows():
        cursor.execute(
            """INSERT INTO zones (location_id, borough, zone, service_zone)
            VALUES (%s, %s, %s, %s)""",
            (row["location_id"], row["borough"], row["zone"],
             row["service_zone"])
        )


def main():
    """Load taxi zones from CSV into PostgreSQL."""
    zones = read_zones()
    connection = connect_db()
    try:
        cursor = connection.cursor()
        # Delete existing zones to avoid duplicates before inserting new zones
        cursor.execute("DELETE FROM zones")
        insert_zones(cursor, zones)
        connection.commit()
        cursor.close()
    finally:
        connection.close()
    print(f"Inserted {len(zones)} rows into the zones table.")


if __name__ == "__main__":
    main()
