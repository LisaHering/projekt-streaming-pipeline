from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext, ProcessWindowFunction, KeyedProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction, AggregateFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration, Time, Configuration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.typeinfo import Types
from typing import NamedTuple
from datetime import datetime
import json
import psycopg2
import os

# --- CONFIGURATION ---

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PICKUP_TOPIC = os.getenv("PICKUP_TOPIC", "pickup_events")
DROPOFF_TOPIC = os.getenv("DROPOFF_TOPIC", "dropoff_events")
PARALLELISM = int(os.getenv("PARALLELISM", "1"))

CHECKPOINT_INTERVAL_MS = int(os.getenv("CHECKPOINT_INTERVAL_MS", "0"))
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints")

OUT_OF_ORDERNESS_SECONDS = 30
MAX_DURATION_SECONDS = 60 * 60 *3
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

class PickupTimestampAssigner(TimestampAssigner):
    """Use pickup time as event time."""

    def extract_timestamp(self, event, record_timestamp):
        dt = datetime.fromisoformat(event["pickup_datetime"])
        return int(dt.timestamp() * 1000)


class DropoffTimestampAssigner(TimestampAssigner):
    """Use dropoff time as event time."""

    def extract_timestamp(self, event, record_timestamp):
        dt = datetime.fromisoformat(event["dropoff_datetime"])
        return int(dt.timestamp() * 1000)

# --- JOIN FUNCTION ---

class JoinTripsFunction(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        pickup_desc = runtime_context.get_state(self._value_state_descriptor("pickup_state"))
        dropoff_desc = runtime_context.get_state(self._value_state_descriptor("dropoff_state"))
        self.pickup_state = pickup_desc
        self.dropoff_state = dropoff_desc

    def _value_state_descriptor(self, name):
        return ValueStateDescriptor(name, Types.STRING())

    def process_element(self, event, ctx):
        if event["event_type"] == "pickup":
            yield from self._handle_pickup(event, ctx)
        else:
            yield from self._handle_dropoff(event, ctx)

    def _handle_pickup(self, pickup, ctx):
        yield {
            "record_type": "lifecycle",
            "signal": "pickup",
            "trip_id": pickup["trip_id"],
            "timestamp": ctx.timestamp()
        }
        self.pickup_state.update(json.dumps(pickup))
        dropoff_json = self.dropoff_state.value()
        if dropoff_json is not None:
            dropoff_ms = int(datetime.fromisoformat(json.loads(dropoff_json)["dropoff_datetime"]).timestamp() * 1000)
            ctx.timer_service().register_event_time_timer(dropoff_ms)
        else:
            ctx.timer_service().register_event_time_timer(ctx.timestamp() + TIMEOUT_SECONDS * 1000)

    def _handle_dropoff(self, dropoff, ctx):
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
            ctx.timer_service().register_event_time_timer(ctx.timestamp() + TIMEOUT_SECONDS * 1000)

    def on_timer(self, timestamp, ctx):
        pickup_json = self.pickup_state.value()
        dropoff_json = self.dropoff_state.value()
        if pickup_json is not None and dropoff_json is not None:
            yield self._build_trip(json.loads(pickup_json), json.loads(dropoff_json))
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
        is_valid = 0 < duration_seconds <= MAX_DURATION_SECONDS
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
            "is_valid": is_valid
        }

    def _build_orphan(self, event):
        return{
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
            "is_valid": False
        }
    
# --- INCREMENTAL AGGREGATION ---

class TripStatsAggregate(AggregateFunction):
    def create_accumulator(self):
        return(0, 0.0, 0.0, 0.0)
    
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
    """Turns pre-aggregated result of one window into metric rows."""

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
    def open(self, runtime_context):
        self.connection = connect_db()
        cursor = self.connection.cursor()
        cursor.execute("""SELECT location_id, borough FROM zones""")
        self.borough_by_zone = dict(cursor.fetchall())
        cursor.close()

    def close(self):
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()

    def map(self, row):
        row["dropoff_borough"] = self.borough_by_zone.get(row["dropoff_zone"], "Unknown")
        return row

BOROUGH_METRICS = {
    "trip_count_by_borough": lambda s: s.count,
    "avg_passengers_by_borough": lambda s: s.passengers / s.count,
    "avg_distance_by_borough": lambda s: s.distance / s.count,
}

# --- AGGREGATION 2: STATISTICS BY TIME OF DAY ---

def dropoff_time_to_time_of_day(dt):
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

LAST_HOUR_METRICS = {
    "avg_price_last_h": lambda s: s.revenue / s.count,
    "revenue_last_h": lambda s: s.revenue,
    "trips_last_h": lambda s: s.count,
}

# --- AGGREGATION 5: UTILIZATION (TAXIS IN SERVICE) ---

GAUGE_WINDOW_SECONDS = MAX_DURATION_SECONDS

class OpenTripsGaugeFunction(KeyedProcessFunction):
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
            ctx.timer_service().register_processing_time_timer(now + 60_000)
            self.timer_registered.update(True)

    def on_timer(self, timestamp, ctx):
        ctx.timer_service().register_processing_time_timer(timestamp + 60000)
        event_now = ctx.timer_service().current_watermark()
        if event_now <= 0:
            return
                
        cutoff = event_now - GAUGE_WINDOW_SECONDS * 1000
        count = 0
        finished = []
        for trip_id , pickup_ms in list(self.pickups.items()):
            dropoff_ms = self.dropoffs.get(trip_id)
            if (dropoff_ms is not None and dropoff_ms <= event_now) or pickup_ms < cutoff:
                finished.append(trip_id)
            elif pickup_ms <= event_now:
                count += 1
        for trip_id in finished:
            self.pickups.remove(trip_id)
            self.dropoffs.remove(trip_id)

        stale_dropoffs = [trip_id for trip_id, dropoff_ms in self.dropoffs.items() if dropoff_ms < cutoff]
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

class InvalidsSinkFunction(MapFunction):
    def open(self, runtime_context):
        self.connection = connect_db()
        self.cursor = self.connection.cursor()

    def map(self, row):
        self.cursor.execute(
            """INSERT INTO invalid_trips
                (trip_id, pickup_zone, dropoff_zone, pickup_datetime, dropoff_datetime,
                duration_seconds, trip_distance, passenger_count, total_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (trip_id) DO NOTHING""",
            (row["trip_id"], row["pickup_zone"], row["dropoff_zone"], row["pickup_datetime"], row["dropoff_datetime"],
            row["duration_seconds"], row["trip_distance"], row["passenger_count"], row["total_amount"])
        )
        self.connection.commit()
        return row

    def close(self):
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()


class AggregatesSinkFunction(MapFunction):
    def open(self, runtime_context):
        self.connection = connect_db()
        self.cursor = self.connection.cursor()

    def map(self, row):
        self.cursor.execute(
            """INSERT INTO aggregates (metric_name, window_start, window_end, dimension, value)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (metric_name, window_start, window_end, dimension)
                DO UPDATE SET value = EXCLUDED.value""",
            (row["metric_name"], row["window_start"], row["window_end"], row["dimension"], row["value"])
        )
        self.connection.commit()
        return row

    def close(self):
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()

# --- PIPELINE ---

def create_environment():
    """Configure Flink environment."""
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

def read_events(env, topic, group_id, timestamp_assigner):
    """Read JSON events from Kafka topic and assign event-time watermarks."""
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(topic)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .set_group_id(group_id)
        .build()
    )
    watermarks = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(OUT_OF_ORDERNESS_SECONDS))
        .with_timestamp_assigner(timestamp_assigner)
    )
    return (
        env.from_source(
            source=source,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name=f"{topic}_source",
        )
        .map(lambda text: json.loads(text))
        .assign_timestamps_and_watermarks(watermarks)
    )

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

    pickups = read_events(env, PICKUP_TOPIC, "pickup_consumer_group", PickupTimestampAssigner())
    dropoffs = read_events(env, DROPOFF_TOPIC, "dropoff_consumer_group", DropoffTimestampAssigner())

    joined_trips = (pickups.union(dropoffs)
        .key_by(lambda event: event["trip_id"])
        .process(JoinTripsFunction())
    )

    trips_only = joined_trips.filter(lambda r: r["record_type"] == "trip")
    lifecycle_signals = joined_trips.filter(lambda r: r["record_type"] == "lifecycle")

    (trips_only.filter(lambda trip: not trip["is_valid"])
        .map(InvalidsSinkFunction()))

    enriched_trips = (trips_only.filter(lambda trip: trip["is_valid"])
        .map(EnrichBoroughFunction())
        .map(add_time_of_day)
    )

    (enriched_trips.key_by(lambda trip: trip["dropoff_borough"])
        .window(TumblingEventTimeWindows.of(Time.hours(24)))
        .aggregate(TripStatsAggregate(), window_function=WindowStatsFunction(BOROUGH_METRICS))
        .map(AggregatesSinkFunction()))

    (enriched_trips.key_by(lambda trip: trip["time_of_day"])
        .window(TumblingEventTimeWindows.of(Time.hours(24)))
        .aggregate(TripStatsAggregate(), window_function=WindowStatsFunction(TIME_OF_DAY_METRICS))
        .map(AggregatesSinkFunction()))

    (enriched_trips.key_by(dropoff_week_key)
        .window(TumblingEventTimeWindows.of(Time.days(7), Time.days(4)))
        .aggregate(TripStatsAggregate(), window_function=WindowStatsFunction(WEEKLY_METRICS))
        .map(AggregatesSinkFunction()))

    (enriched_trips.key_by(lambda trip: "all")
        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(LAST_HOUR_SLIDE_MINUTES)))
        .aggregate(TripStatsAggregate(), window_function=WindowStatsFunction(LAST_HOUR_METRICS))
        .map(AggregatesSinkFunction()))

    (lifecycle_signals.key_by(lambda signal: "all")
        .process(OpenTripsGaugeFunction())
        .map(AggregatesSinkFunction()))
    
    env.execute("Taxi Flink Streaming Job")

if __name__ == "__main__":
    main()