#!/bin/bash

# Load environment variables with defaults
KAFKA_PORT=${KAFKA_INTERNAL_PORT:-9092}
TOPIC=${KAFKA_TOPIC:-earthquake_raw}

kafka-topics --bootstrap-server localhost:${KAFKA_PORT} --create --topic ${TOPIC} --partitions 3 --replication-factor 1
