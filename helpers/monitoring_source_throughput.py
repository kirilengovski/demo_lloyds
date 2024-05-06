import logging
# Source to Kafka monitoring and alerting


def check_message_trhoughput(expected_throughput, deviation, trhoughput_per_interval, intetval):
    if trhoughput_per_interval <= (expected_throughput - deviation):
        message = "Throughput from source TOO LOW. " \
                  f"Throughput is {trhoughput_per_interval} messages per {intetval} seconds."
        logging.warning(message)
        logging.warning(f"ALERT sent to analysis@email.com.")
    else:
        logging.info("Throughput OK")

