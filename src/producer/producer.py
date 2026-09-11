from kafka import KafkaProducer
import json
import time

# connect to Kafka broker
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)       

test_nachricht = {"nachricht" : "Hallo, das ist eine Testnachricht!", "timestamp" : time.time()}
producer.send('test-topic', value=test_nachricht)

producer.flush()

print("Testnachricht erfolgreich an Kafka gesendet.")
