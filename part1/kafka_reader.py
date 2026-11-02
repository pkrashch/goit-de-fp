"""Виводить на екран дані з вихідного Kafka-топіку (для скриншота). Ctrl+C — зупинка."""
import json

from kafka import KafkaConsumer

from configs import MY_NAME, TOPIC_ENRICHED, kafka_python_params

consumer = KafkaConsumer(
    TOPIC_ENRICHED,
    **kafka_python_params(),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id=f"{MY_NAME}_enriched_reader",
)

print(f"Listening to '{TOPIC_ENRICHED}'...")
try:
    for message in consumer:
        print(json.dumps(message.value, ensure_ascii=False))
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
