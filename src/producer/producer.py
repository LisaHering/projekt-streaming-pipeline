from kafka import KafkaProducer
import json
import time
import psycopg2
import os
import heapq

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
cursor = connection.cursor(name="trip_cursor")
cursor.itersize = 10000
cursor.execute("""
    SELECT pickup_datetime, dropoff_datetime, pickup_zone, dropoff_zone, passenger_count, trip_distance, total_amount
    FROM raw_trips
    ORDER BY pickup_datetime
""")

pending_dropoffs = []
trip_id = 0
previous_time = None

def send_event(topic, data, sort_time):
    global previous_time
    if previous_time is not None:
        gap_seconds = (sort_time - previous_time).total_seconds()
        wait = min(gap_seconds / speed_factor, 30.0)
        if wait > 0:
            time.sleep(wait)
    producer.send(topic, data)
    previous_time = sort_time
    print(f"{sort_time} -> {topic} trip {data['trip_id']}")        

for pickup_datetime, dropoff_datetime, pickup_zone, dropoff_zone, \
        passenger_count, trip_distance, total_amount in cursor:
    trip_id += 1

    while pending_dropoffs and pending_dropoffs[0][0] <= pickup_datetime:
        dropoff_time, dropoff_data = heapq.heappop(pending_dropoffs)
        send_event("dropoff_events", dropoff_data, dropoff_time)

    send_event("pickup_events", {
        "trip_id": trip_id,
        "event_type": "pickup",
        "pickup_datetime": pickup_datetime.isoformat(),
        "pickup_zone": pickup_zone,
        "passenger_count": passenger_count
    }, pickup_datetime)

    heapq.heappush(pending_dropoffs, (dropoff_datetime, {
        "trip_id": trip_id,
        "event_type": "dropoff",
        "dropoff_datetime": dropoff_datetime.isoformat(),
        "dropoff_zone": dropoff_zone,
        "trip_distance": float(trip_distance),
        "total_amount": float(total_amount)
    }))

while pending_dropoffs:
    dropoff_time, dropoff_data = heapq.heappop(pending_dropoffs)
    send_event("dropoff_events", dropoff_data, dropoff_time)

producer.flush()
cursor.close()
connection.close()
print("All events sent to Kafka.")