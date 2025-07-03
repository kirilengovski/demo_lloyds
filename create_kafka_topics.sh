#!/bin/bash

# Define the Kafka container name
KAFKA_CONTAINER_NAME="kafka"

# List of topics to create
TOPICS=(
  "currency_validation_requests"
  "enriched_transactions"
  "insights"
  "insights_requests"
  "transactions"
  "dlq" # Dead Letter Queue
)

# Create each topic inside the Kafka container
for TOPIC in "${TOPICS[@]}"; do
  docker exec -it "$KAFKA_CONTAINER_NAME" kafka-topics --create \
    --topic "$TOPIC" \
    --bootstrap-server "localhost:9092" \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists
  echo "Creating topic: $TOPIC"
done

echo "All topics created successfully!"