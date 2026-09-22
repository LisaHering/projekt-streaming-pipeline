from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedCoProcessFunction, MapFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.typeinfo import Types
from datetime import datetime
import json
import psycopg2

class PickupTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        dt = datetime.fromisoformat(event["pickup_datetime"])
        return int(dt.timestamp() * 1000)

class DropoffTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_timestamp):
        dt = datetime.fromisoformat(event["dropoff_datetime"])
        return int(dt.timestamp() * 1000)

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# --- KAFKA SOURCES & STREAMS ---

pickup_source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topics("pickup_events") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .set_group_id("pickup_consumer_group") \
    .build()

pickup_stream = env.from_source(
    source=pickup_source,
    watermark_strategy=WatermarkStrategy.no_watermarks(),
    source_name="pickup_source"
)

parsed_pickups = pickup_stream.map(lambda text: json.loads(text))

pickups_with_time = parsed_pickups.assign_timestamps_and_watermarks(
    WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(PickupTimestampAssigner())
)     

dropoff_source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topics("dropoff_events") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .set_group_id("dropoff_consumer_group") \
    .build()

dropoff_stream = env.from_source(
    source=dropoff_source,
    watermark_strategy=WatermarkStrategy.no_watermarks(),
    source_name="dropoff_source"
)

parsed_dropoffs = dropoff_stream.map(lambda text: json.loads(text))

dropoffs_with_time = parsed_dropoffs.assign_timestamps_and_watermarks(
    WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(30))
        .with_timestamp_assigner(DropoffTimestampAssigner())
)

# --- JOIN FUNCTION ---

MAX_DURATION_SECONDS = 60 * 60 * 3

class JoinTripsFunction(KeyedCoProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        pickup_desc = runtime_context.get_state(self._value_state_descriptor("pickup_state"))
        dropoff_desc = runtime_context.get_state(self._value_state_descriptor("dropoff_state"))
        self.pickup_state = pickup_desc
        self.dropoff_state = dropoff_desc

    def _value_state_descriptor(self, name):
        return ValueStateDescriptor(name, Types.STRING())

    def process_element1(self, pickup, ctx):
        dropoff_json = self.dropoff_state.value()
        if dropoff_json is not None:
            dropoff = json.loads(dropoff_json)
            yield self._build_trip(pickup, dropoff)
            self.dropoff_state.clear()
        else:
            self.pickup_state.update(json.dumps(pickup))

    def process_element2(self, dropoff, ctx):
        pickup_json = self.pickup_state.value()
        if pickup_json is not None:
            pickup = json.loads(pickup_json)
            yield self._build_trip(pickup, dropoff)
            self.pickup_state.clear()
        else:
            self.dropoff_state.update(json.dumps(dropoff))

    def _build_trip(self, pickup, dropoff):
        pickup_dt = datetime.fromisoformat(pickup["pickup_datetime"])
        dropoff_dt = datetime.fromisoformat(dropoff["dropoff_datetime"])
        duration_seconds = (dropoff_dt - pickup_dt).total_seconds()
        is_valid = 0 < duration_seconds <= MAX_DURATION_SECONDS
        return {
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

# --- POSTGRES SINK FUNCTION ---

class PostgresSinkFunction(MapFunction):
    def open(self, runtime_context):
        self.connection = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="taxi",
            user="taxi_user",   
            password="taxi_pass"
        )
        self.cursor = self.connection.cursor()

    def map(self, row):
        self.cursor.execute(
            """INSERT INTO invalid_trips 
                (trip_id, pickup_zone, dropoff_zone, pickup_datetime, dropoff_datetime, 
                duration_seconds, trip_distance, passenger_count, total_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
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

# --- STREAM CONNECTION & EXECUTION ---

joined_trips = pickups_with_time.key_by(lambda event: event["trip_id"]) \
    .connect(dropoffs_with_time.key_by(lambda event: event["trip_id"])) \
    .process(JoinTripsFunction())

joined_trips \
    .map(lambda trip:   f"{'VALID' if trip['is_valid'] else 'INVALID'} "
                        f"Trip {trip['trip_id']} from {trip['pickup_zone']} to {trip['dropoff_zone']} " 
                        f"took {trip['duration_seconds']/60:.1f} min for {trip['trip_distance']} miles " 
                        f"passengers: {trip['passenger_count']}, total amount: ${trip['total_amount']}") \
    .print()

joined_trips \
    .filter(lambda trip: not trip["is_valid"]) \
    .map(PostgresSinkFunction())

env.execute("Taxi Flink Streaming Job")