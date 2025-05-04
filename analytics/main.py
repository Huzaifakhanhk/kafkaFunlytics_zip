
# Python Kafka Consumer for Analysis

from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'humor-events',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='analytics-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Python Consumer ready! Analyzing data with a pinch of humor! 😄")

for message in consumer:
    print(f"Analysis: Funny message detected - {message.value}")
