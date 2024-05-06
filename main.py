import json
import logging
import threading

from confluent_kafka import Producer, Consumer, KafkaError

from helpers.db import insert_into_postgres
from helpers.config import CONSUMER, KAFKA_CONFIG, DB_PARAMS, transactions, basic_shema
from helpers.generate_transactions import generate_transactions
from threading import Thread
from helpers.monitoring_source_throughput import check_message_trhoughput
from helpers.logging_config import ColoredFormatter
from time import time, sleep
from helpers.validate_schema import validate_schema
from helpers.enrichment import enrich_data
import sys

root_logger = logging.getLogger()
if root_logger.handlers:
    root_logger.handlers = []

# Configure the root logger with the custom formatter
root_logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(ColoredFormatter('%(asctime)s - %(levelname)s - %(message)s'))
root_logger.addHandler(console_handler)


producer = Producer(KAFKA_CONFIG)
consumer = Consumer(KAFKA_CONFIG, **CONSUMER)
dlq_consumer = Consumer(KAFKA_CONFIG, **CONSUMER)

trs = generate_transactions(transactions)

message_count = 0
start_time = time()

expected_throughput = 4
deviation = 2

logging.info("start")


def monitor_throughput(interval=10):
    global message_count
    """
    Monitor total messages produced per interval and print the count.
    """
    while True:
        prev_message_count = message_count
        sleep(interval)
        current_message_count = message_count
        total_messages = current_message_count - prev_message_count
        logging.debug(f"Total messages produced in the last {interval} seconds: {total_messages}")
        check_message_trhoughput(expected_throughput, deviation, total_messages, interval)


def consume_messages():

    # ----------- MAIN CONSUMER FOR MESSAGES WITH OK SCHEMA ----------- #

    consumer.subscribe(['transactions'])
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                logging.error(f"Consumer Error: {msg.error()}")
                break
        logging.debug(f"Consumed message: {msg.key()}: {msg.value()}")

        # ----------- ENRICHMENT STEP -----------#

        logging.debug(f"RAW_DATA_AUDIT UPDATE - Ingesting key columns to (BigQuery) "
                      f"auditing_transactions.raw_data_audit")

        enriched_message = enrich_data(msg)

        logging.debug(
            f"ENRICHED_DATA_AUDIT UPDATE - Ingesting key columns to (BigQuery) auditing_transactions.enriched_data_audit")
        # ----------- INSERT INTO DATABASE AFTER ENRICHMENT ----------- #
        if enriched_message is not None:
            insert_into_postgres(enriched_message, DB_PARAMS)

        consumer.commit(msg)


def consume_dlq_messages():
    dlq_consumer.subscribe(['dlq'])
    try:
        while True:
            msg = dlq_consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                    break
            logging.warning(f"Message received from DLQ: {msg.key()}: {msg.value()}")
            # Process the message from DLQ (e.g., resend to Kafka topic)
            logging.warning(f"Adding to deadletter bucket at gcs:// ...")
            dlq_consumer.commit(msg)
    finally:
        consumer.close()


if __name__ == '__main__':
    ok_consumer_thread = threading.Thread(target=consume_messages)
    ok_consumer_thread.start()

    dlq_consumer_thread = threading.Thread(target=consume_dlq_messages)
    dlq_consumer_thread.start()

    monitor_thread = Thread(target=monitor_throughput)
    monitor_thread.daemon = True
    monitor_thread.start()

    for transaction in transactions:
        message_key = str(transaction['transaction_id'])
        message_value = json.dumps(transaction)

        '''
        Code to produce the messages coming from source to the right topic.
        Based on schema validation, data will end up either in the transactions or the deadletter topic.
        '''
        if validate_schema(message_value, basic_shema):
            producer.produce('transactions', key=message_key, value=message_value)
            logging.debug(f"Produced message on topic transactions: {message_key}: {message_value}")
        else:
            # Send failed messages to DLQ
            producer.produce('dlq', key=message_key, value=message_value)
            logging.error(f"SCHEMA VALIDATION FAILED on message: {message_key}: {message_value}")
            logging.error("Message will not be produced in transactions topic ...")
            logging.error("Sending to deadletter topic instead ...")
        message_count += 1
        sleep(0.01)

    # Flush producer's message queue
    producer.flush()

    # Wait for consumer threads to finish
    ok_consumer_thread.join()
    dlq_consumer_thread.join()

    # Close Kafka producers and consumers
    producer.close()
    consumer.close()
    dlq_consumer.close()
