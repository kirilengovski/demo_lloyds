from confluent_kafka.admin import AdminClient


topic_name = "transactions"

DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'kiko123',
    'host': 'localhost',
}

KAFKA_CONFIG_OLD = {
    'bootstrap.servers': 'pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092',
    'bootstrap.servers': 'pkc-l6wr6.europe-west2.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'E5N6WPW7PIASSHDS',
    'sasl.password': 'lKm4FF9hReq1ZxdfaOsOv00vx+VBoCdUT+6/iREuj4SO9Ca/KADxqGqbIxNh8Ulc'
}

KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'security.protocol': 'PLAINTEXT',  # No SASL/SSL for local Kafka
}

CONSUMER = {
    'group.id': 'first_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False  # Disable auto commit of offsets
}

basic_shema = ["transaction_id", "amount", "currency", "timestamp"]

transactions = [
    {"transaction_id": "001", "amount": 500.25, "currency": "USD", "timestamp": "2024-05-06T12:30:00Z"},
    {"transaction_id": "002", "amount": 1000.50, "currency": "EUR", "timestamp": "2024-05-06T13:45:00Z"},
    {"transaction_id": "003", "amount": 750.75, "currency": "GBP", "timestamp": "2024-05-06T15:00:00Z"},
    {"transaction_id": "004", "amount": 500.25, "currency": "USD", "timestamp": "2024-05-06T12:30:00Z"},
    {"transaction_id": "005", "amount": 1000.50, "currency": "EUR", "timestamp": "2024-05-06T13:45:00Z"},
    {"transaction_id": "006", "amount": 750.75, "currency": "GBP", "timestamp": "2024-05-06T15:00:00Z"},
    {"transaction_id": "007", "amount": 500.25, "currency": "USD", "timestamp": "2024-05-06T12:30:00Z"},
    {"transaction_id": "008", "amount": 1000.50, "currency": "EUR", "timestamp": "2024-05-06T13:45:00Z"},
    # bad currency:
    {"transaction_id": "008", "amount": 1000.50, "currency": "MKD", "timestamp": "2024-05-06T13:45:00Z"},
    {"transaction_id": "009", "amount": 750.75, "currency": "GBP", "timestamp": "2024-05-06T15:00:00Z"},
    # schema invalid:
    {"transaction_id": "012", "country": "North Macedonia", "amount": 750.75, "currency": "GBP", "timestamp": "2024-05-06T15:00:00Z"},
    {"transaction_id": "010", "amount": 500.25, "currency": "USD", "timestamp": "2024-05-06T12:30:00Z"},
    {"transaction_id": "011", "amount": 1000.50, "currency": "EUR", "timestamp": "2024-05-06T13:45:00Z"},
    {"transaction_id": "012", "amount": 750.75, "currency": "GBP", "timestamp": "2024-05-06T15:00:00Z"}

]

admin_client = AdminClient(KAFKA_CONFIG)

topic_metadata = admin_client.list_topics(topic=topic_name).topics.get(topic_name)
