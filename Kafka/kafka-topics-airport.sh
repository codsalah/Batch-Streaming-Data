#!/bin/bash
# Load environment variables with defaults
KAFKA_PORT=${KAFKA_INTERNAL_PORT:-9092}
TOPIC=${KAFKA_TOPIC_AIRPORT:-airport_stream}

echo "Creating Kafka topic: ${TOPIC}"
kafka-topics --bootstrap-server localhost:${KAFKA_PORT} --create --topic ${TOPIC} --partitions 3 --replication-factor 1 --if-not-exists
