"""Створення Kafka-топіків для частини 1."""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import MY_NAME, TOPIC_ENRICHED, TOPIC_EVENT_RESULTS, kafka_python_params

admin_client = KafkaAdminClient(**kafka_python_params())

for name in (TOPIC_EVENT_RESULTS, TOPIC_ENRICHED):
    try:
        admin_client.create_topics([NewTopic(name=name, num_partitions=2, replication_factor=1)])
        print(f"Topic '{name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{name}' already exists.")

print("\nMy topics:")
[print(topic) for topic in admin_client.list_topics() if MY_NAME in topic]
admin_client.close()
