import subprocess
import logging
import json
from confluent_kafka import Consumer, Producer
from helpers.config import KAFKA_CONFIG

# Kafka setup
producer = Producer(KAFKA_CONFIG)
consumer = Consumer({
    **KAFKA_CONFIG,
    'group.id': 'insights_agent',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['insights_requests'])

def generate_insights_with_ollama(transaction):
    try:
        # Use Ollama to analyze the transaction
        prompt = f"Analyze this transaction and provide insights with a single sentence as a quick summary: {json.dumps(transaction)}"
        result = subprocess.run(
            ["ollama", "run", "llama2"],
            input=prompt,
            text=True,
            capture_output=True
        )
        response = result.stdout.strip()
        logging.info(f"Ollama Insights Response: {response}")
        return response
    except Exception as e:
        logging.error(f"Error generating insights with Ollama: {e}")
        return "No insights available"

def process_insights_requests():
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error(f"Consumer error: {msg.error()}")
            continue

        transaction = json.loads(msg.value())
        logging.info(f"Received enriched transaction: {transaction}")

        # Generate insights using Ollama
        insights = generate_insights_with_ollama(transaction)

        # Produce the insights to the insights topic
        producer.produce('insights', key=transaction['transaction_id'], value=json.dumps({"transaction_id": transaction['transaction_id'], "insights": insights}))
        producer.flush()
        logging.info(f"Generated insights for transaction {transaction['transaction_id']} and sent to 'insights'.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_insights_requests()