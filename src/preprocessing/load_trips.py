import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

csv_paths = [
    "/Users/lisa/Dokumente/nyc_taxi_2022_01/nyc_taxi_2022_01_woche_1.csv",
    "/Users/lisa/Dokumente/nyc_taxi_2022_01/nyc_taxi_2022_01_woche_2.csv",
    "/Users/lisa/Dokumente/nyc_taxi_2022_01/nyc_taxi_2022_01_woche_3.csv",
    "/Users/lisa/Dokumente/nyc_taxi_2022_01/nyc_taxi_2022_01_woche_4.csv",
    "/Users/lisa/Dokumente/nyc_taxi_2022_01/nyc_taxi_2022_01_woche_5.csv",
]
load_dotenv()
chunk_size = 50000
commit_every = 50000

connection = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="taxi",
    user="taxi_user",   
    password=os.getenv("DB_PASSWORD")
)
cursor = connection.cursor()

cursor.execute("SELECT location_id FROM zones")
valid_zones = {row[0] for row in cursor.fetchall()}

inserted = 0
rejected = {
    "missing_values": 0,
    "time_order": 0,
    "distance": 0,
    "price": 0,
    "passengers": 0,
    "invalid_zones": 0
}

processed = 0

for csv_path in csv_paths:
    print(f"--- Lade {csv_path} ---")
    reader = pd.read_csv(csv_path, chunksize=chunk_size)
    for chunk in reader:
        for _, row in chunk.iterrows():
            processed += 1
            pickup = pd.to_datetime(row["pickup_datetime"], errors="coerce")
            dropoff = pd.to_datetime(row["dropoff_datetime"], errors="coerce")
            if pd.isna(pickup) or pd.isna(dropoff) \
                or pd.isna(row["passenger_count"]) or pd.isna(row["trip_distance"]) \
                or pd.isna(row["total_amount"]) \
                or pd.isna(row["pickup_location_id"]) or pd.isna(row["dropoff_location_id"]):
                rejected["missing_values"] += 1
                continue
            if pickup >= dropoff:
                rejected["time_order"] += 1
                continue
            if row["trip_distance"] <= 0:
                rejected["distance"] += 1
                continue
            if row["total_amount"] <= 0:
                rejected["price"] += 1
                continue
            if not (1 <= row["passenger_count"] <= 8):
                rejected["passengers"] += 1
                continue
            if int(row["pickup_location_id"]) not in valid_zones \
                or int(row["dropoff_location_id"]) not in valid_zones:
                rejected["invalid_zones"] += 1
                continue
            cursor.execute(
                """INSERT INTO raw_trips 
                (pickup_datetime, dropoff_datetime, passenger_count, pickup_zone, dropoff_zone, 
                trip_distance, total_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (pickup, dropoff, int(row["passenger_count"]), int(row["pickup_location_id"]), 
                int(row["dropoff_location_id"]), row["trip_distance"], row["total_amount"])
            )
            inserted += 1
            if inserted % commit_every == 0:
                connection.commit()

connection.commit()
cursor.close()
connection.close()

print(f"Processed {processed} rows, inserted {inserted} rows into the raw_trips table.")
print(f"Rejected rows: {sum(rejected.values())}")
for reason, count in rejected.items():
    print(f"  {reason}: {count}")