"""Flink streaming job for NYC taxi events.

Reads pickup and dropoff events from Kafka, validates and joins them into trips
per trip_id and computes real-time metrics. Results are written to PostgreSQL;
unreadable messages and invalid trips stored in quarantine tables.
"""

import json
import os
from datetime import datetime
from typing import NamedTuple

import psycopg2
from pyflink.common import Configuration, Duration, Time, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (KafkaOffsetsInitializer,
                                                 KafkaSource)
from pyflink.datastream.functions import (AggregateFunction,
                                          KeyedProcessFunction, MapFunction,
                                          ProcessWindowFunction)
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.datastream.window import (SlidingEventTimeWindows,
                                       TumblingEventTimeWindows)

# --- CONFIGURATION ---
# Override settings via environment variables in docker-compose.

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PICKUP_TOPIC = os.getenv("PICKUP_TOPIC", "pickup_events")
DROPOFF_TOPIC = os.getenv("DROPOFF_TOPIC", "dropoff_events")
PARALLELISM = int(os.getenv("PARALLELISM", "1"))

CHECKPOINT_INTERVAL_MS = int(os.getenv("CHECKPOINT_INTERVAL_MS", "0"))
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints")

OUT_OF_ORDERNESS_SECONDS = 30
# trips longer than 3 h treated as data errors
MAX_DURATION_SECONDS = 60 * 60 * 3
# unmatched pickups / dropoffs given up after 48 h to prevent large join state
TIMEOUT_SECONDS = 60 * 60 * 48
LAST_HOUR_SLIDE_MINUTES = 5


def connect_db():
    """Open new PostgreSQL connection with configurations."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "taxi"),
        user=os.getenv("DB_USER", "taxi_user"),
        password=os.environ["DB_PASSWORD"],
    )


# --- EVENT TIME ---
# Windows based on event not reading time
# -> results independent of processing speed

class PickupTimestampAssigner(TimestampAssigner):
    """Use pickup time as event time of pickup event."""

    def extract_timestamp(self, event, record_timestamp):
        dt = datetime.fromisoformat(event["pickup_datetime"])
        return int(dt.timestamp() * 1000)


class DropoffTimestampAssigner(TimestampAssigner):
    """Use dropoff time as event time of dropoff event."""

    def extract_timestamp(self, event, record_timestamp):
        dt = datetime.fromisoformat(event["dropoff_datetime"])
        return int(dt.timestamp() * 1000)


# --- VALIDATION ---
# Incoming stream not trusted -> every message checked before processing
# Invalid messages stored to prevent crashing

PICKUP_FIELDS = ("trip_id", "event_type", "pickup_datetime",
                 "pickup_zone", "passenger_count")
DROPOFF_FIELDS = ("trip_id", "event_type", "dropoff_datetime",
                  "dropoff_zone", "trip_distance", "total_amount")
MAX_PASSENGERS = 8


def is_number(value):
    """Return True for int and float values, bool not accepted."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_event(event, event_type):
    """Check fields, event types & value ranges, provide rejection reason."""
    if not isinstance(event, dict):
        return "not_a_json_object"
    if event.get("event_type") != event_type:
        return "unexpected_event_type"
    fields = PICKUP_FIELDS if event_type == "pickup" else DROPOFF_FIELDS
    for field in fields:
        if event.get(field) is None:
            return f"missing_{field}"
    try:
        datetime.fromisoformat(event[f"{event_type}_datetime"])
    except (TypeError, ValueError):
        return "invalid_timestamp"
    if not isinstance(event[f"{event_type}_zone"], int):
        return "invalid_zone"
    if event_type == "pickup":
        passengers = event["passenger_count"]
        if not is_number(passengers) or not 1 <= passengers <= MAX_PASSENGERS:
            return "invalid_passenger_count"
    else:
        distance = event["trip_distance"]
        amount = event["total_amount"]
        if not is_number(distance) or distance <= 0:
            return "invalid_trip_distance"
        if not is_number(amount) or amount <= 0:
            return "invalid_total_amount"
    return None


def parse_event(raw, topic, event_type):
    """Turn raw Kafka message into event or rejection record."""
    try:
        event = json.loads(raw)
    except (TypeError, ValueError):
        reason = "invalid_json"
    else:
        reason = validate_event(event, event_type)
        if reason is None:
            event["record_type"] = "event"
            return event
    return {"record_type": "rejected", "topic": topic,
            "reason": reason, "raw_payload": raw}


# --- JOIN FUNCTION ---

class JoinTripsFunction(KeyedProcessFunction):
    """Join pickup and dropoff event of same trip, keyed by trip_id.

    Uses union() + one function instead of KeyedCoProcessFunction, which failed
    in PyFlink with "A serializer has already been registered for the state".
    """

    def open(self, runtime_context):
        self.pickup_state = runtime_context.get_state(
            ValueStateDescriptor("pickup_state", Types.STRING()))
        self.dropoff_state = runtime_context.get_state(
            ValueStateDescriptor("dropoff_state", Types.STRING()))

    def process_element(self, event, ctx):
        if event["event_type"] == "pickup":
            yield from self._handle_pickup(event, ctx)
        else:
            yield from self._handle_dropoff(event, ctx)

    def _handle_pickup(self, pickup, ctx):
        """Store pickup, emit trip once dropoff is known."""
        yield {
            "record_type": "lifecycle",
            "signal": "pickup",
            "trip_id": pickup["trip_id"],
            "timestamp": ctx.timestamp()
        }
        self.pickup_state.update(json.dumps(pickup))
        dropoff_json = self.dropoff_state.value()
        if dropoff_json is not None:
            dropoff = json.loads(dropoff_json)
            dropoff_time = datetime.fromisoformat(dropoff["dropoff_datetime"])
            dropoff_ms = int(dropoff_time.timestamp() * 1000)
            ctx.timer_service().register_event_time_timer(dropoff_ms)
        else:
            ctx.timer_service().register_event_time_timer(
                ctx.timestamp() + TIMEOUT_SECONDS * 1000)

    def _handle_dropoff(self, dropoff, ctx):
        """Emit trip if pickup is known, otherwise store dropoff."""
        yield {
            "record_type": "lifecycle",
            "signal": "dropoff",
            "trip_id": dropoff["trip_id"],
            "timestamp": ctx.timestamp()
        }
        pickup_json = self.pickup_state.value()
        if pickup_json is not None:
            pickup = json.loads(pickup_json)
            yield self._build_trip(pickup, dropoff)
            self.pickup_state.clear()
        else:
            self.dropoff_state.update(json.dumps(dropoff))
            ctx.timer_service().register_event_time_timer(
                ctx.timestamp() + TIMEOUT_SECONDS * 1000)

    def on_timer(self, timestamp, ctx):
        """Emit delayed trip or orphan (after timeout)."""
        pickup_json = self.pickup_state.value()
        dropoff_json = self.dropoff_state.value()
        if pickup_json is not None and dropoff_json is not None:
            yield self._build_trip(json.loads(pickup_json),
                                   json.loads(dropoff_json))
            self.pickup_state.clear()
            self.dropoff_state.clear()
            return
        if pickup_json is not None:
            yield self._build_orphan(json.loads(pickup_json))
            self.pickup_state.clear()
        if dropoff_json is not None:
            yield self._build_orphan(json.loads(dropoff_json))
            self.dropoff_state.clear()

    def _build_trip(self, pickup, dropoff):
        pickup_dt = datetime.fromisoformat(pickup["pickup_datetime"])
        dropoff_dt = datetime.fromisoformat(dropoff["dropoff_datetime"])
        duration_seconds = (dropoff_dt - pickup_dt).total_seconds()
        if duration_seconds <= 0:
            invalid_reason = "non_positive_duration"
        elif duration_seconds > MAX_DURATION_SECONDS:
            invalid_reason = "duration_over_3h"
        else:
            invalid_reason = None
        return {
            "record_type": "trip",
            "trip_id": pickup["trip_id"],
            "pickup_zone": pickup["pickup_zone"],
            "dropoff_zone": dropoff["dropoff_zone"],
            "pickup_datetime": pickup["pickup_datetime"],
            "dropoff_datetime": dropoff["dropoff_datetime"],
            "duration_seconds": duration_seconds,
            "trip_distance": dropoff["trip_distance"],
            "passenger_count": pickup["passenger_count"],
            "total_amount": dropoff["total_amount"],
            "is_valid": invalid_reason is None,
            "invalid_reason": invalid_reason,
        }

    def _build_orphan(self, event):
        """Build invalid trip record for event without counterpart."""
        if "pickup_datetime" in event:
            reason = "missing_dropoff"
        else:
            reason = "missing_pickup"
        return {
            "record_type": "trip",
            "trip_id": event["trip_id"],
            "pickup_zone": event.get("pickup_zone"),
            "dropoff_zone": event.get("dropoff_zone"),
            "pickup_datetime": event.get("pickup_datetime"),
            "dropoff_datetime": event.get("dropoff_datetime"),
            "duration_seconds": None,
            "trip_distance": event.get("trip_distance"),
            "passenger_count": event.get("passenger_count"),
            "total_amount": event.get("total_amount"),
            "is_valid": False,
            "invalid_reason": reason
        }


# --- INCREMENTAL AGGREGATION ---
# Windows keep running sums instead of all trips
# to prevent out-of-memory crashes.

class TripStatsAggregate(AggregateFunction):
    """Running sums per window: count, passengers, distance, revenue."""

    def create_accumulator(self):
        return (0, 0.0, 0.0, 0.0)

    def add(self, trip, acc):
        return (acc[0] + 1,
                acc[1] + trip["passenger_count"],
                acc[2] + trip["trip_distance"],
                acc[3] + trip["total_amount"])

    def get_result(self, acc):
        return acc

    def merge(self, a, b):
        return (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3])


class TripStats(NamedTuple):
    """Readable view on accumulator of TripStatsAggregate."""

    count: int
    passengers: float
    distance: float
    revenue: float


class WindowStatsFunction(ProcessWindowFunction):
    """Turn pre-aggregated result of one window into metric rows."""

    def __init__(self, metrics):
        super().__init__()
        self.metrics = metrics

    def process(self, key, context, elements):
        stats = TripStats(*next(iter(elements)))
        window_start = datetime.fromtimestamp(context.window().start / 1000)
        window_end = datetime.fromtimestamp(context.window().end / 1000)
        for metric_name, compute in self.metrics.items():
            yield {
                "metric_name": metric_name,
                "window_start": window_start,
                "window_end": window_end,
                "dimension": key,
                "value": compute(stats)
            }


# --- AGGREGATION 1: STATISTICS BY BOROUGHS ---

class EnrichBoroughFunction(MapFunction):
    """Add dropoff borough to each trip."""

    def open(self, runtime_context):
        self.connection = connect_db()
        cursor = self.connection.cursor()
        cursor.execute("""SELECT location_id, borough FROM zones""")
        self.borough_by_zone = dict(cursor.fetchall())
        cursor.close()

    def close(self):
        if getattr(self, 'connection', None):
            self.connection.close()

    def map(self, row):
        row["dropoff_borough"] = self.borough_by_zone.get(
            row["dropoff_zone"], "Unknown")
        return row


BOROUGH_METRICS = {
    "trip_count_by_borough": lambda s: s.count,
    "avg_passengers_by_borough": lambda s: s.passengers / s.count,
    "avg_distance_by_borough": lambda s: s.distance / s.count,
}


# --- AGGREGATION 2: STATISTICS BY TIME OF DAY ---

def dropoff_time_to_time_of_day(dt):
    """Map dropoff hour to corresponding time of day bucket."""
    hour = dt.hour
    if 5 <= hour < 11:
        return "morning"
    elif 11 <= hour < 14:
        return "noon"
    elif 14 <= hour < 17:
        return "afternoon"
    elif 17 <= hour < 23:
        return "evening"
    else:
        return "night"


TIME_OF_DAY_METRICS = {
    "trip_count_by_time_of_day": lambda s: s.count,
    "avg_passengers_by_time_of_day": lambda s: s.passengers / s.count,
    "avg_distance_by_time_of_day": lambda s: s.distance / s.count,
}


# --- AGGREGATION 3: WEEKLY TRIPS & REVENUE ---

def dropoff_time_to_week(dt):
    """Return ISO week label."""
    year, week, _ = dt.isocalendar()
    return f"{year}-W{week:02d}"


WEEKLY_METRICS = {
    "trips_per_week": lambda s: s.count,
    "revenue_by_week": lambda s: s.revenue,
    "revenue_per_mile_by_week": (
        lambda s: s.revenue / s.distance if s.distance > 0 else 0.0
    ),
}


# --- AGGREGATION 4: REVENUE LAST HOUR ---
# Sliding window: 1 h, updated every 5 min

LAST_HOUR_METRICS = {
    "avg_price_last_h": lambda s: s.revenue / s.count,
    "revenue_last_h": lambda s: s.revenue,
    "trips_last_h": lambda s: s.count,
}


# --- AGGREGATION 5: UTILIZATION (TAXIS IN SERVICE) ---

# capped at 3 h, consistent with validity rule
GAUGE_WINDOW_SECONDS = MAX_DURATION_SECONDS


class OpenTripsGaugeFunction(KeyedProcessFunction):
    """Count taxis currently in service (picked up, not yet dropped off)."""

    def open(self, runtime_context):
        self.pickups = runtime_context.get_map_state(
            MapStateDescriptor("gauge_pickups", Types.STRING(), Types.LONG())
        )
        self.dropoffs = runtime_context.get_map_state(
            MapStateDescriptor("gauge_dropoffs", Types.STRING(), Types.LONG())
        )
        self.timer_registered = runtime_context.get_state(
            ValueStateDescriptor("gauge_timer_registered", Types.BOOLEAN())
        )

    def process_element(self, value, ctx):
        trip_id = str(value["trip_id"])
        if value["signal"] == "pickup":
            self.pickups.put(trip_id, value["timestamp"])
        else:
            self.dropoffs.put(trip_id, value["timestamp"])

        if self.timer_registered.value() is None:
            now = ctx.timer_service().current_processing_time()
            ctx.timer_service().register_processing_time_timer(now + 60000)
            self.timer_registered.update(True)

    def on_timer(self, timestamp, ctx):
        now = ctx.timer_service().current_processing_time()
        ctx.timer_service().register_processing_time_timer(now + 60000)
        event_now = ctx.timer_service().current_watermark()
        if event_now <= 0:
            return

        cutoff = event_now - GAUGE_WINDOW_SECONDS * 1000
        # Batched read instead of one state request per pickup
        # to improve latency
        dropoffs = dict(self.dropoffs.items())
        count = 0
        finished = []
        for trip_id, pickup_ms in list(self.pickups.items()):
            dropoff_ms = dropoffs.get(trip_id)
            dropped_off = dropoff_ms is not None and dropoff_ms <= event_now
            if dropped_off or pickup_ms < cutoff:
                finished.append(trip_id)
            elif pickup_ms <= event_now:
                count += 1
        for trip_id in finished:
            self.pickups.remove(trip_id)
            self.dropoffs.remove(trip_id)

        stale_dropoffs = [trip_id for trip_id, dropoff_ms
                          in self.dropoffs.items() if dropoff_ms < cutoff]
        for trip_id in stale_dropoffs:
            self.dropoffs.remove(trip_id)

        yield {
            "metric_name": "open_trips",
            "window_start": datetime.fromtimestamp(cutoff / 1000),
            "window_end": datetime.fromtimestamp(event_now / 1000),
            "dimension": "all",
            "value": count
        }


# --- POSTGRES SINK FUNCTIONS ---
# All sinks are idempotent: Kafka streams are re-read from beginning
# after restart -> duplicates must be prevented
# Each row committed immediately -> results visible in real time

class PostgresSinkFunction(MapFunction):
    """Write each record as row."""

    sql = None

    def open(self, runtime_context):
        self.connection = connect_db()
        self.cursor = self.connection.cursor()

    def map(self, row):
        self.cursor.execute(self.sql, row)
        self.connection.commit()
        return row

    def close(self):
        if getattr(self, 'cursor', None):
            self.cursor.close()
        if getattr(self, 'connection', None):
            self.connection.close()


class RejectedSinkFunction(PostgresSinkFunction):
    """Store messages that failed validation."""

    sql = """INSERT INTO rejected_events (topic, reason, raw_payload)
        VALUES (%(topic)s, %(reason)s, %(raw_payload)s)
        ON CONFLICT DO NOTHING"""


class InvalidsSinkFunction(PostgresSinkFunction):
    """Store trips that violate business rules."""

    sql = """INSERT INTO invalid_trips
            (trip_id, pickup_zone, dropoff_zone, pickup_datetime,
            dropoff_datetime, duration_seconds, trip_distance,
            passenger_count, total_amount, invalid_reason)
        VALUES (%(trip_id)s, %(pickup_zone)s, %(dropoff_zone)s,
                %(pickup_datetime)s, %(dropoff_datetime)s,
                %(duration_seconds)s, %(trip_distance)s,
                %(passenger_count)s, %(total_amount)s, %(invalid_reason)s)
        ON CONFLICT (trip_id) DO NOTHING"""


class AggregatesSinkFunction(PostgresSinkFunction):
    """Upsert metric rows; recomputed window overrides old value."""

    sql = """INSERT INTO aggregates
            (metric_name, window_start, window_end, dimension, value)
        VALUES (%(metric_name)s, %(window_start)s, %(window_end)s,
                %(dimension)s, %(value)s)
        ON CONFLICT (metric_name, window_start, window_end, dimension)
        DO UPDATE SET value = EXCLUDED.value"""


# --- PIPELINE ---

def create_environment():
    """Configure Flink environment with restart strategy and checkpoints."""
    config = Configuration()
    config.set_string("restart-strategy.type", "fixed-delay")
    config.set_string("restart-strategy.fixed-delay.attempts", "3")
    config.set_string("restart-strategy.fixed-delay.delay", "10 s")
    if CHECKPOINT_INTERVAL_MS > 0:
        config.set_string("execution.checkpointing.dir", CHECKPOINT_DIR)
        config.set_string(
            "execution.checkpointing.tolerable-failed-checkpoints", "5")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(PARALLELISM)
    if CHECKPOINT_INTERVAL_MS > 0:
        env.enable_checkpointing(CHECKPOINT_INTERVAL_MS)
    return env


def read_events(env, topic, event_type, group_id, timestamp_assigner):
    """Read topic, validate messages and assign event time."""
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(topic)
        # Always start from beginning, replay after restart
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .set_group_id(group_id)
        .build()
    )
    watermarks = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(
            Duration.of_seconds(OUT_OF_ORDERNESS_SECONDS))
        .with_timestamp_assigner(timestamp_assigner)
    )
    parsed = (
        env.from_source(
            source=source,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name=f"{topic}_source",
        )
        .map(lambda raw: parse_event(raw, topic, event_type))
    )
    events = (
        parsed.filter(lambda r: r["record_type"] == "event")
        .assign_timestamps_and_watermarks(watermarks)
    )
    rejected = parsed.filter(lambda r: r["record_type"] == "rejected")
    return events, rejected


def add_time_of_day(trip):
    """Attach time-of-day bucket of dropoff to trip."""
    dropoff = datetime.fromisoformat(trip["dropoff_datetime"])
    return {**trip, "time_of_day": dropoff_time_to_time_of_day(dropoff)}


def dropoff_week_key(trip):
    """Return ISO week of dropoff."""
    dropoff = datetime.fromisoformat(trip["dropoff_datetime"])
    return dropoff_time_to_week(dropoff)


def main():
    """Build streaming pipeline and start Flink job."""
    env = create_environment()

    pickups, rejected_pickups = read_events(
        env, PICKUP_TOPIC, "pickup", "pickup_consumer_group",
        PickupTimestampAssigner())
    dropoffs, rejected_dropoffs = read_events(
        env, DROPOFF_TOPIC, "dropoff", "dropoff_consumer_group",
        DropoffTimestampAssigner())

    rejected_pickups.union(rejected_dropoffs).map(RejectedSinkFunction())

    joined_trips = (
        pickups.union(dropoffs)
        .key_by(lambda event: event["trip_id"])
        .process(JoinTripsFunction())
    )

    trips_only = joined_trips.filter(lambda r: r["record_type"] == "trip")
    lifecycle_signals = joined_trips.filter(
        lambda r: r["record_type"] == "lifecycle")

    trips_only.filter(lambda trip: not trip["is_valid"]).map(
        InvalidsSinkFunction())

    enriched_trips = (
        trips_only.filter(lambda trip: trip["is_valid"])
        .map(EnrichBoroughFunction())
        .map(add_time_of_day)
    )

    (enriched_trips
        .key_by(lambda trip: trip["dropoff_borough"])
        .window(TumblingEventTimeWindows.of(Time.hours(24)))
        .aggregate(TripStatsAggregate(),
                   window_function=WindowStatsFunction(BOROUGH_METRICS))
        .map(AggregatesSinkFunction()))

    (enriched_trips
        .key_by(lambda trip: trip["time_of_day"])
        .window(TumblingEventTimeWindows.of(Time.hours(24)))
        .aggregate(TripStatsAggregate(),
                   window_function=WindowStatsFunction(TIME_OF_DAY_METRICS))
        .map(AggregatesSinkFunction()))

    # Flink aligns windows to a Thursday (1970-01-01) -> 4 days offset (Monday)
    (enriched_trips
        .key_by(dropoff_week_key)
        .window(TumblingEventTimeWindows.of(Time.days(7), Time.days(4)))
        .aggregate(TripStatsAggregate(),
                   window_function=WindowStatsFunction(WEEKLY_METRICS))
        .map(AggregatesSinkFunction()))

    (enriched_trips
        .key_by(lambda trip: "all")
        .window(SlidingEventTimeWindows.of(
            Time.hours(1), Time.minutes(LAST_HOUR_SLIDE_MINUTES)))
        .aggregate(TripStatsAggregate(),
                   window_function=WindowStatsFunction(LAST_HOUR_METRICS))
        .map(AggregatesSinkFunction()))

    (lifecycle_signals
        .key_by(lambda signal: "all")
        .process(OpenTripsGaugeFunction())
        .map(AggregatesSinkFunction()))

    env.execute("Taxi Flink Streaming Job")


if __name__ == "__main__":
    main()
