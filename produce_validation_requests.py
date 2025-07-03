# dummy producer script to send test messages to the Kafka topic 'currency_validation_requests'

from confluent_kafka import Producer
import json

KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(KAFKA_CONFIG)

def produce_test_message():
    message = {
        "transaction_id": "003",
        "currency": "BGN",
        "amount": 300
    }
    producer.produce('currency_validation_requests', key=message['transaction_id'], value=json.dumps(message))
    producer.flush()
    print(f"Produced message: {message}")

if __name__ == "__main__":
    produce_test_message()