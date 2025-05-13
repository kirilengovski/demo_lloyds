import psycopg2

DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'kiko123',
    'host': 'localhost',
}

try:
    conn = psycopg2.connect(**DB_PARAMS)
    print("Connection successful!")
    conn.close()
except Exception as e:
    print(f"Connection failed: {e}")