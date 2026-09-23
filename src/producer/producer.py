from kafka import KafkaProducer
import json
import time
import psycopg2
import os

speed_factor = 600

# connect to Kafka broker
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)       

connection = psycopg2.connect(
    host="postgres",
    port=5432,
    dbname="taxi",
    user="taxi_user",   
    password=os.getenv("DB_PASSWORD")
)
cursor = connection.cursor()

cursor.execute("""
    SELECT pickup_datetime, dropoff_datetime, pickup_zone, dropoff_zone, passenger_count, trip_distance, total_amount
    FROM raw_trips
    ORDER BY pickup_datetime
    LIMIT 1000
""")
trips = cursor.fetchall()
cursor.close()
connection.close()

events = []
trip_id = 0
for trip in trips:
    trip_id += 1
    pickup_datetime, dropoff_datetime, pickup_zone, dropoff_zone, \
        passenger_count, trip_distance, total_amount = trip

    events.append({
        "sort_time": pickup_datetime,
        "topic": "pickup_events",
        "data": {
            "trip_id": trip_id,
            "event_type": "pickup",
            "pickup_datetime": pickup_datetime.isoformat(),
            "pickup_zone": pickup_zone,
            "passenger_count": passenger_count
        }
    })

    events.append({
        "sort_time": dropoff_datetime,
        "topic": "dropoff_events",
        "data": {
            "trip_id": trip_id,
            "event_type": "dropoff",
            "dropoff_datetime": dropoff_datetime.isoformat(),
            "dropoff_zone": dropoff_zone,
            "trip_distance": float(trip_distance),
            "total_amount": float(total_amount)
        }
    })

events.sort(key=lambda e: e["sort_time"])

previous_time = None
for e in events:
    if previous_time is not None:
        gap_seconds = (e["sort_time"] - previous_time).total_seconds()
        wait = gap_seconds / speed_factor
        wait = min(wait, 5.0)
        if wait > 0:
            time.sleep(wait)
    producer.send(e["topic"], e["data"])
    previous_time = e["sort_time"]
    print(f"{e['sort_time']} -> {e['topic']} trip {e['data']['trip_id']}")

producer.flush()
print("All events sent to Kafka.")