# Demo Lloyds Project

This project demonstrates a data pipeline for processing and enriching transactions using Kafka and PostgreSQL. It includes features like schema validation, data enrichment, and monitoring.

## Requirements

- Python 3.8+
- Docker and Docker Compose
- Confluent Kafka and PostgreSQL (can be started with `docker-compose.yml`)

## Setup

1. Clone the repository and navigate to the project folder:
```bash
git clone <repository-url>
cd demo_lloyds
```

2. Install dependencies
- python -m venv venv
- source venv/bin/activate
- pip install -r requirements.txt

3. Start Kafka and PostgreSQL using Docker Compose:
- docker-compose up

4. Run the pipeline:
- python main.py

## Environment Variables Setup

This project uses a `.env` file to manage sensitive configuration such as database and Kafka credentials.

### 1. Create a `.env` file in the project root:

```env
DB_USER=postgres
DB_PASSWORD=your_db_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
# Add any other required variables here
```

### 2. Do NOT commit your `.env` file

The `.env` file is listed in `.gitignore` and should never be committed to version control.

### 3. How environment variables are loaded

The project uses [`python-dotenv`](https://pypi.org/project/python-dotenv/) to automatically load variables from `.env` when running scripts.

If you add new variables, restart your shell or re-run your scripts to pick up changes.

## Features
- Kafka Integration: Produces and consumes messages.
- Schema Validation: Ensures data integrity.
- Data Enrichment: Adds metadata to transactions.
- PostgreSQL Storage: Saves processed data.
- Monitoring: Tracks throughput and consumer lag.