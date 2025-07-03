import psycopg2
import os
import sys
from dotenv import load_dotenv
load_dotenv()

DB_PARAMS = {
    "user": os.environ.get("DB_USER", "postgres"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", 5432)),
    "database": os.environ.get("DB_NAME", "postgres")
}

def main():
    if not DB_PARAMS['password']:
        print("Error: DB_PASSWORD environment variable is not set.")
        sys.exit(1)
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            print("Connection successful!")
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    main()