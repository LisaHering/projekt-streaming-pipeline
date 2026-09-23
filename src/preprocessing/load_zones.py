import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

csv_path = "/Users/lisa/Dokumente/nyc_taxi_2022_01/taxi_zone_lookup.csv"
zones = pd.read_csv(csv_path)
load_dotenv()

zones = zones.rename(columns={
    "LocationID": "location_id",
    "Borough": "borough",
    "Zone": "zone",
    "service_zone": "service_zone"
})  

connection = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="taxi",
    user="taxi_user",
    password=os.getenv("DB_PASSWORD")
)
cursor = connection.cursor()

for _, row in zones.iterrows():
    cursor.execute(
        "INSERT INTO zones (location_id, borough, zone, service_zone) VALUES (%s, %s, %s, %s)",
        (row["location_id"], row["borough"], row["zone"], row["service_zone"])
    )

connection.commit()
cursor.close()
connection.close()  

print(f"Inserted {len(zones)} rows into the zones table.")