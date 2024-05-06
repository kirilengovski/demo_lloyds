import logging


def validate_schema(message, schema):
    message_schema = eval(message)

    if len(message_schema) != len(schema):
        return False

    for key in schema:
        if key not in message_schema:
            logging.error(f"Schema missmatch detected on message id: {message['transaction_id']}")
            return False
    return True
