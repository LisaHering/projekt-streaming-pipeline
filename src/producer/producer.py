from kafka import KafkaProducer
import json
import time
import psycopg2

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
    password="taxi_pass"
)
cursor = connection.cursor()

cursor.execute("""
    SELECT pickup_datetime, dropoff_datetime, pickup_zone, dropoff_zone, passenger_count, trip_distance, total_amount
    FROM raw_trips
    ORDER BY pickup_datetime
    LIMIT 5
""")
trips = cursor.fetchall()

trip_id = 0
for trip in trips:
    trip_id += 1
    pickup_datetime, dropoff_datetime, pickup_zone, dropoff_zone, passenger_count, trip_distance, total_amount = trip

    pickup_event = {
        "trip_id": trip_id,
        "event_type": "pickup",
        "pickup_datetime": pickup_datetime.isoformat(),
        "pickup_zone": pickup_zone,
        "passenger_count": passenger_count
    }

    dropoff_event = {
        "trip_id": trip_id,
        "event_type": "dropoff",
        "dropoff_datetime": dropoff_datetime.isoformat(),
        "dropoff_zone": dropoff_zone,
        "trip_distance": float(trip_distance),
        "total_amount": float(total_amount)
    }

    producer.send("pickup_events", pickup_event)
    producer.send("dropoff_events", dropoff_event)
    producer.flush()

print("All events sent to Kafka.")

cursor.close()
connection.close()