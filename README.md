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


## Features
- Kafka Integration: Produces and consumes messages.
- Schema Validation: Ensures data integrity.
- Data Enrichment: Adds metadata to transactions.
- PostgreSQL Storage: Saves processed data.
- Monitoring: Tracks throughput and consumer lag.