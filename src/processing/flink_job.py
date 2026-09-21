from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from datetime import datetime
import json

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
pickups_with_time \
    .map(lambda event: f"PICKUP trip {event['trip_id']} at {event['pickup_datetime']}") \
    .print()

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
dropoffs_with_time \
    .map(lambda event: f"DROPOFF trip {event['trip_id']} at {event['dropoff_datetime']}, price: ${event['total_amount']}") \
    .print()

env.execute("Taxi Flink Streaming Job")