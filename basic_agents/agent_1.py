import subprocess
import logging
import json
from confluent_kafka import Producer, Consumer
from helpers.config import KAFKA_CONFIG, CONSUMER  # Assuming CONSUMER is defined in your config

# Kafka setup
producer = Producer(KAFKA_CONFIG)
consumer = Consumer(KAFKA_CONFIG, **CONSUMER)  # Aligning with your existing consumer pattern
consumer.subscribe(['currency_validation_requests'])

# Shared context for MCP
context = []

def validate_currency_with_ollama(currency):
    global context
    try:
        # Add the new query to the context
        context.append({"role": "user", "content": f"Is '{currency}' a valid currency? If yes, provide single sentence details."})

        # Format the context into a single prompt for Ollama
        prompt = "\n".join([f"{entry['role']}: {entry['content']}" for entry in context])

        # Use Ollama's CLI to query the local LLM
        result = subprocess.run(
            ["ollama", "run", "llama2"],
            input=prompt,
            text=True,
            capture_output=True
        )
        response = result.stdout.strip()

        # Add the model's response to the context
        context.append({"role": "assistant", "content": response})

        logging.info(f"Ollama Response for currency '{currency}': {response}")
        return "valid" in response.lower(), response
    except Exception as e:
        logging.error(f"Error validating currency with Ollama: {e}")
        return False, "Error"

def process_currency_validation_requests():
    logging.info("Starting to process messages from 'currency_validation_requests'...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            logging.debug("No messages received.")
            continue
        if msg.error():
            logging.error(f"Consumer error: {msg.error()}")
            continue

        logging.info(f"Received message: {msg.value().decode('utf-8')}")
        transaction = json.loads(msg.value())
        currency = transaction['currency']
        is_valid, response = validate_currency_with_ollama(currency)

        # Add validation result to the transaction
        transaction['currency_valid'] = is_valid
        transaction['validation_details'] = response

        # Send the result to the insights agent
        producer.produce('insights_requests', key=transaction['transaction_id'], value=json.dumps(transaction))
        producer.flush()
        logging.info(f"Validated currency for transaction {transaction['transaction_id']} and sent to 'insights_requests'.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    process_currency_validation_requests()