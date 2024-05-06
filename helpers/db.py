import json
import psycopg2
import logging


def insert_into_postgres(msg, db_params):
    transaction_data = msg
    # print(transaction_data)
    # Connect to PostgreSQL database
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        # Insert data into PostgreSQL table
        cur.execute("INSERT INTO demo_transactions.transactions (transaction_id, amount, currency, transaction_timestamp, "
                    "big_transaction) VALUES (%s, %s, %s, %s, %s)",
                    (transaction_data['transaction_id'], transaction_data['amount'], transaction_data['currency'],
                     transaction_data['timestamp'], transaction_data['big_transaction']))
        conn.commit()
        logging.info(f"Successfully inserted {transaction_data['transaction_id']} into PostgreSQL")
    except (Exception, psycopg2.Error) as error:
        logging.error("Error while connecting to PostgreSQL or inserting data:", error)
    finally:
        if conn:
            cur.close()
            conn.close()
