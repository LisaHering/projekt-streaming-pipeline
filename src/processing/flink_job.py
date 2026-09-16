from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import WatermarkStrategy
import json

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
parsed_pickups \
    .map(lambda event: f"PICKUP trip {event['trip_id']} in zone {event['pickup_zone']}") \
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
parsed_dropoffs \
    .map(lambda event: f"DROPOFF trip {event['trip_id']} in zone {event['dropoff_zone']}, price: ${event['total_amount']}") \
    .print()

env.execute("Taxi Flink Streaming Job")