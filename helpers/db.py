import json
import psycopg2
import logging

def create_table_if_not_exists(db_params):
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        cursor.execute("""
            DROP TABLE IF EXISTS demo_transactions.transactions;
            CREATE SCHEMA IF NOT EXISTS demo_transactions;
            CREATE TABLE IF NOT EXISTS demo_transactions.transactions (
                transaction_id VARCHAR(50) PRIMARY KEY,
                amount NUMERIC(10, 2),
                currency VARCHAR(10),
                transaction_timestamp TIMESTAMP,
                big_transaction BOOLEAN
            );
        """)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Table demo_transactions.transactions ensured.")
    except Exception as e:
        logging.error(f"Error ensuring table exists: {e}")

def insert_into_postgres(transaction_data, db_params):
    try:
        # Ensure required keys exist
        if 'timestamp' not in transaction_data:
            raise KeyError("Missing 'timestamp' in transaction data")

        # Connect to PostgreSQL database
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
        logging.error(f"Error while connecting to PostgreSQL or inserting data: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()
