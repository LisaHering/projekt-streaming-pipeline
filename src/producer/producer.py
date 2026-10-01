"""Kafka producer that ingests NYC taxi trips as event stream.

Reads trips from raw_trips ordered by pickup time and sends one pickup and
one dropoff event per trip (in event-time order). Gaps between events are
replayed in accelerated real time.
"""

import heapq
import json
import os
import sys
import time
import uuid

import psycopg2
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

# --- CONFIGURATION ---
# Override settings via environment variables in docker-compose.

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PICKUP_TOPIC = os.getenv("PICKUP_TOPIC", "pickup_events")
DROPOFF_TOPIC = os.getenv("DROPOFF_TOPIC", "dropoff_events")
# 600 -> 1 h of trips replayed in 6 s
SPEED_FACTOR = float(os.getenv("SPEED_FACTOR", "600"))
# cap long waits so replay does not stall
MAX_WAIT_SECONDS = 30.0
LOG_EVERY = 100000

TRIPS_QUERY = """
    SELECT pickup_datetime, dropoff_datetime, pickup_zone, dropoff_zone,
            passenger_count, trip_distance, total_amount
    FROM raw_trips
    ORDER BY pickup_datetime
"""


def connect_db():
    """Open new PostgreSQL connection with configurations."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "taxi"),
        user=os.getenv("DB_USER", "taxi_user"),
        password=os.environ["DB_PASSWORD"],
    )


def create_producer():
    """Create Kafka producer that waits for broker confirmation."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        # broker confirms every write, failed sends are retried
        acks="all",
    )


def topic_has_events(topic):
    """Return True if topic contains messages."""
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return False
        offsets = consumer.end_offsets(
            [TopicPartition(topic, p) for p in partitions])
        return any(offset > 0 for offset in offsets.values())
    finally:
        consumer.close()


class EventSender:
    """Send events with accelerated real-time gaps and count errors."""

    def __init__(self, producer):
        self.producer = producer
        self.previous_time = None
        self.sent = 0
        self.errors = 0

    def send(self, topic, event, event_time):
        if self.previous_time is not None:
            gap_seconds = (event_time - self.previous_time).total_seconds()
            wait = min(gap_seconds / SPEED_FACTOR, MAX_WAIT_SECONDS)
            if wait > 0:
                time.sleep(wait)
        future = self.producer.send(topic, key=event["trip_id"], value=event)
        future.add_errback(self._on_error)
        self.previous_time = event_time
        self.sent += 1
        if self.sent % LOG_EVERY == 0:
            print(f"{self.sent} events sent, event time {event_time}")

    def _on_error(self, exc):
        self.errors += 1
        print(f"ERROR: event could not be sent: {exc}", file=sys.stderr)


def replay_trips(cursor, sender):
    """Send all pickup and dropoff events in event-time order."""
    # Dropoffs wait in min-heap until their time is reached
    pending_dropoffs = []
    for (pickup_datetime, dropoff_datetime, pickup_zone, dropoff_zone,
         passenger_count, trip_distance, total_amount) in cursor:
        # Source data has no ID -> unique even across producer restarts
        trip_id = str(uuid.uuid4())
        while pending_dropoffs and pending_dropoffs[0][0] <= pickup_datetime:
            dropoff_time, _, dropoff = heapq.heappop(pending_dropoffs)
            sender.send(DROPOFF_TOPIC, dropoff, dropoff_time)

        sender.send(PICKUP_TOPIC, {
            "trip_id": trip_id,
            "event_type": "pickup",
            "pickup_datetime": pickup_datetime.isoformat(),
            "pickup_zone": pickup_zone,
            "passenger_count": passenger_count
        }, pickup_datetime)

        heapq.heappush(pending_dropoffs, (dropoff_datetime, trip_id, {
            "trip_id": trip_id,
            "event_type": "dropoff",
            "dropoff_datetime": dropoff_datetime.isoformat(),
            "dropoff_zone": dropoff_zone,
            "trip_distance": float(trip_distance),
            "total_amount": float(total_amount)
        }))

    while pending_dropoffs:
        dropoff_time, _, dropoff = heapq.heappop(pending_dropoffs)
        sender.send(DROPOFF_TOPIC, dropoff, dropoff_time)


def main():
    """Replay all trips once; refuse to run if topics are already filled."""
    for topic in (PICKUP_TOPIC, DROPOFF_TOPIC):
        if topic_has_events(topic):
            print(f"Topic {topic} already contains events. Nothing sent. "
                  "Delete the topics for a new replay.")
            return

    producer = create_producer()
    connection = connect_db()
    sender = EventSender(producer)
    try:
        cursor = connection.cursor(name="trip_cursor")
        cursor.itersize = 10000
        cursor.execute(TRIPS_QUERY)
        replay_trips(cursor, sender)
        cursor.close()
        producer.flush()
    finally:
        producer.close()
        connection.close()

    print(f"All events sent: {sender.sent}, errors: {sender.errors}")
    if sender.errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
