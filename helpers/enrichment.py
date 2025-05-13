from helpers.config import KAFKA_CONFIG
from confluent_kafka import Producer
import json
import logging

producer = Producer(KAFKA_CONFIG)
supported_currencies = ["USD", "GBP", "EUR"]


def enrich_data(message):
    try:
        dict_message = json.loads(message.value())

        # If a currency used in the transaction is not supported then send it back to Kafka in the deadletter topic
        if dict_message["currency"] not in supported_currencies:
            message_key = str(dict_message['transaction_id'])
            message_value = json.dumps(dict_message)
            producer.produce('dlq', key=message_key, value=message_value)
            logging.error("ENRICHMENT CANNOT BE COMPLETED")
            logging.error(f"CURRENCY ({dict_message['currency']}) NOT SUPPORTED for transaction: {message_key}: {message_value}")
            logging.error("Sending back to deadletter topic ...")
            producer.flush()
            return None

        # Check for big transactions and set the flag for big transaction to true or false
        dict_message['big_transaction'] = dict_message['amount'] > 1000

        logging.info(f"Successfully enriched message {dict_message['transaction_id']} with (big_transaction) "
                     f"value: {dict_message['big_transaction']}")
        return dict_message
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse message: {message.value()}, error: {e}")
        return None


