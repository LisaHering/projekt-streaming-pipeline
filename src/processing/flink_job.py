from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream import ProcessWindowFunction, KeyedProcessFunction, OutputTag
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedCoProcessFunction, MapFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.typeinfo import Types
from datetime import datetime
import json
import psycopg2
import os

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
env.enable_checkpointing(60_000)

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
TIMEOUT_SECONDS = 60 * 60 * 48

# Side-Output-Kanal: meldet, wann eine Fahrt "offen" (Pickup da, Dropoff fehlt noch)
# bzw. "geschlossen" (Dropoff kam / Timeout) ist. Wird fuer den Live-Gauge "Autos im
# Einsatz" gebraucht, getrennt vom normalen Trip-Output.
# LIFECYCLE_TAG = OutputTag("trip-lifecycle", Types.STRING())

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
            ctx.timer_service().register_event_time_timer(ctx.timestamp() + TIMEOUT_SECONDS * 1000)
            # ctx.output(LIFECYCLE_TAG, json.dumps({
            #     "signal": "open",
            #     "trip_id": pickup["trip_id"],
            #     "pickup_datetime": pickup["pickup_datetime"]}))

    def process_element2(self, dropoff, ctx):
        pickup_json = self.pickup_state.value()
        if pickup_json is not None:
            pickup = json.loads(pickup_json)
            yield self._build_trip(pickup, dropoff)
            self.pickup_state.clear()
            # ctx.output(LIFECYCLE_TAG, json.dumps({
            #     "signal": "close",
            #     "trip_id": dropoff["trip_id"]
            # }))
        else:
            self.dropoff_state.update(json.dumps(dropoff))
            ctx.timer_service().register_event_time_timer(ctx.timestamp() + TIMEOUT_SECONDS * 1000)

    def on_timer(self, timestamp, ctx):
        pickup_json = self.pickup_state.value()
        if pickup_json is not None:
            pickup = json.loads(pickup_json)
            yield self._build_orphan(pickup)
            self.pickup_state.clear()
            # ctx.output(LIFECYCLE_TAG, json.dumps({
            #     "signal": "close",
            #     "trip_id": pickup["trip_id"]
            # }))
        dropoff_json = self.dropoff_state.value()
        if dropoff_json is not None:
            yield self._build_orphan(json.loads(dropoff_json))
            self.dropoff_state.clear()

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

    def _build_orphan(self, event):
        return{
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

# --- BOROUGH ENRICHMENT ---

class EnrichBoroughFunction(MapFunction):
    def open(self, runtime_context):
        self.connection = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="taxi",
            user="taxi_user",   
            password=os.getenv("DB_PASSWORD")
        )
        cursor = self.connection.cursor()
        cursor.execute("""SELECT location_id, borough FROM zones""")
        self.borough_by_zone = dict(cursor.fetchall())
        cursor.close()

    def map(self, row):
        row["dropoff_borough"] = self.borough_by_zone.get(row["dropoff_zone"], "Unknown")
        return row

# --- AGGREGATION 1: STATISTICS BY BOROUGHS ---

class BoroughStatsFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        elements = list(elements)
        count = len(elements)
        avg_passengers = sum(event["passenger_count"] for event in elements) / count
        avg_distance = sum(event["trip_distance"] for event in elements) / count
        window_start = datetime.fromtimestamp(context.window().start / 1000)
        window_end = datetime.fromtimestamp(context.window().end / 1000)
        yield {"metric_name": "trip_count_by_borough", "window_start": window_start,
               "window_end": window_end, "dimension": key, "value": count}
        yield {"metric_name": "avg_passengers_by_borough", "window_start": window_start,
               "window_end": window_end, "dimension": key, "value": avg_passengers}
        yield {"metric_name": "avg_distance_by_borough", "window_start": window_start,
               "window_end": window_end, "dimension": key, "value": avg_distance}

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

class TimeOfDayStatsFunction(ProcessWindowFunction):        
    def process(self, key, context, elements):
        elements = list(elements)
        count = len(elements)
        avg_passengers = sum(event["passenger_count"] for event in elements) / count
        avg_distance = sum(event["trip_distance"] for event in elements) / count
        window_start = datetime.fromtimestamp(context.window().start / 1000)
        window_end = datetime.fromtimestamp(context.window().end / 1000)
        yield {"metric_name": "trip_count_by_time_of_day", "window_start": window_start,
               "window_end": window_end, "dimension": key, "value": count}
        yield {"metric_name": "avg_passengers_by_time_of_day", "window_start": window_start,
                "window_end": window_end, "dimension": key, "value": avg_passengers}
        yield {"metric_name": "avg_distance_by_time_of_day", "window_start": window_start,
                "window_end": window_end, "dimension": key, "value": avg_distance}

# --- POSTGRES SINK FUNCTIONS ---

class InvalidsSinkFunction(MapFunction):
    def open(self, runtime_context):
        self.connection = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="taxi",
            user="taxi_user",
            password=os.getenv("DB_PASSWORD")
        )
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
        self.connection = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="taxi",
            user="taxi_user",
            password=os.getenv("DB_PASSWORD")
        )
        self.cursor = self.connection.cursor()

    def map(self, row):
        self.cursor.execute(
            """INSERT INTO aggregates (metric_name, window_start, window_end, dimension, value)
                VALUES (%s, %s, %s, %s, %s)""",
            (row["metric_name"], row["window_start"], row["window_end"], row["dimension"], row["value"])
        )
        self.connection.commit()
        return row

    def close(self):
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'connection') and self.connection:
            self.connection.close()


# # --- AGGREGATION 1+2: FAHRTEN/STUNDE + DURCHSCHNITTSPREIS/STUNDE (Tumbling, 1h) ---

# class HourlyStatsFunction(ProcessWindowFunction):
#     def process(self, key, context, elements):
#         elements = list(elements)
#         count = len(elements)
#         avg_price = sum(e["total_amount"] for e in elements) / count
#         window_start = datetime.fromtimestamp(context.window().start / 1000)
#         window_end = datetime.fromtimestamp(context.window().end / 1000)
#         yield {"metric_name": "trips_per_hour", "window_start": window_start,
#                "window_end": window_end, "borough": None, "value": count}
#         yield {"metric_name": "avg_price_per_hour", "window_start": window_start,
#                "window_end": window_end, "borough": None, "value": avg_price}

# # --- AGGREGATION 3: GLEITENDER DURCHSCHNITT DES PREISES (Sliding, 2h Fenster, alle 30min neu) ---

# class MovingAvgPriceFunction(ProcessWindowFunction):
#     def process(self, key, context, elements):
#         elements = list(elements)
#         if not elements:
#             return
#         avg_price = sum(e["total_amount"] for e in elements) / len(elements)
#         window_start = datetime.fromtimestamp(context.window().start / 1000)
#         window_end = datetime.fromtimestamp(context.window().end / 1000)
#         yield {"metric_name": "moving_avg_price", "window_start": window_start,
#                "window_end": window_end, "borough": None, "value": avg_price}

 
# # --- AGGREGATION 5: AUTOS IM EINSATZ (Live-Gauge, letzte 3h, alle 60s aktualisiert) ---

# # Eine Fahrt gilt eh nach MAX_DURATION_SECONDS als ungueltig, deshalb dasselbe Zeitfenster
# # fuer "noch offen" verwenden.
# GAUGE_WINDOW_SECONDS = MAX_DURATION_SECONDS

# class OpenTripsGaugeFunction(KeyedProcessFunction):
#     def open(self, runtime_context):
#         self.open_trips = runtime_context.get_map_state(
#             MapStateDescriptor("open_trips", Types.STRING(), Types.LONG())
#         )
#         self.timer_registered = runtime_context.get_state(
#             ValueStateDescriptor("gauge_timer_registered", Types.BOOLEAN())
#         )

#     def process_element(self, value, ctx):
#         signal = json.loads(value)
#         if signal["signal"] == "open":
#             pickup_ms = int(datetime.fromisoformat(signal["pickup_datetime"]).timestamp() * 1000)
#             self.open_trips.put(str(signal["trip_id"]), pickup_ms)
#         elif signal["signal"] == "close":
#             self.open_trips.remove(str(signal["trip_id"]))

#         # Periodischen Timer nur einmal anstossen, er verlaengert sich in on_timer selbst
#         if self.timer_registered.value() is None:
#             now = ctx.timer_service().current_processing_time()
#             ctx.timer_service().register_processing_time_timer(now + 60_000)
#             self.timer_registered.update(True)

#     def on_timer(self, timestamp, ctx):
#         cutoff = timestamp - GAUGE_WINDOW_SECONDS * 1000
#         stale = [trip_id for trip_id, pickup_ms in self.open_trips.items() if pickup_ms < cutoff]
#         for trip_id in stale:
#             self.open_trips.remove(trip_id)

#         count = sum(1 for _ in self.open_trips.items())
#         yield {
#             "metric_name": "open_trips",
#             "window_start": None,
#             "window_end": datetime.fromtimestamp(timestamp / 1000),
#             "borough": None,
#             "value": count
#         }
#         ctx.timer_service().register_processing_time_timer(timestamp + 60_000)

# --- STREAM CONNECTION & EXECUTION ---

joined_trips = pickups_with_time.key_by(lambda event: event["trip_id"]) \
    .connect(dropoffs_with_time.key_by(lambda event: event["trip_id"])) \
    .process(JoinTripsFunction())

joined_trips \
    .map(lambda trip:   f"{'VALID' if trip['is_valid'] else 'INVALID'} "
                        f"Trip {trip['trip_id']} from {trip['pickup_zone']} to {trip['dropoff_zone']} "
                        f"took {trip['duration_seconds']/60 if trip['duration_seconds'] is not None else 'n/a'} min "
                        f"for {trip['trip_distance']} miles "
                        f"passengers: {trip['passenger_count']}, total amount: ${trip['total_amount']}") \
    .print()

joined_trips \
    .filter(lambda trip: not trip["is_valid"]) \
    .map(InvalidsSinkFunction())

enriched_trips = joined_trips \
    .filter(lambda trip: trip["is_valid"]) \
    .map(EnrichBoroughFunction()) \
    .map(lambda trip: {**trip, "time_of_day": dropoff_time_to_time_of_day(datetime.fromisoformat(trip["dropoff_datetime"]))})

enriched_trips.key_by(lambda trip: trip["dropoff_borough"]) \
    .window(TumblingEventTimeWindows.of(Time.hours(24))) \
    .process(BoroughStatsFunction()) \
    .map(AggregatesSinkFunction())

enriched_trips.key_by(lambda trip: trip["time_of_day"]) \
    .window(TumblingEventTimeWindows.of(Time.hours(24))) \
    .process(TimeOfDayStatsFunction()) \
    .map(AggregatesSinkFunction())


# enriched_trips.key_by(lambda t: "all") \
#     .window(TumblingEventTimeWindows.of(Time.hours(1))) \
#     .process(HourlyStatsFunction()) \
#     .map(AggregatesSinkFunction())

# enriched_trips.key_by(lambda t: "all") \
#     .window(SlidingEventTimeWindows.of(Time.hours(2), Time.minutes(30))) \
#     .process(MovingAvgPriceFunction()) \
#     .map(AggregatesSinkFunction())



# joined_trips.get_side_output(LIFECYCLE_TAG) \
#     .key_by(lambda x: "all") \
#     .process(OpenTripsGaugeFunction()) \
#     .map(AggregatesSinkFunction())

env.execute("Taxi Flink Streaming Job")
