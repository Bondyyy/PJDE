from kafka import KafkaConsumer
from config.database_config import get_kafka_config

configKafka = get_kafka_config()
consumer = KafkaConsumer("BondyPJDE", 
                         bootstrap_servers=configKafka["bootstrap_servers"])

msg_pack = consumer.poll(timeout_ms=500)
for tp, messages in msg_pack.items():
    for message in messages:
        print(f"Received message: {message.value.decode('utf-8')}")