#!/bin/bash

# =========================
# Kafka Topic for WolfX Seismic WebSocket Stream
# =========================
#file name Kafka-topic-wolf_seismic_stream.sh 

KAFKA_PORT=${KAFKA_INTERNAL_PORT:-9092}
TOPIC=${KAFKA_TOPIC_WOLF:-wolf_seismic_stream}

kafka-topics \
  --bootstrap-server localhost:${KAFKA_PORT} \
  --create \
  --topic ${TOPIC} \
  --partitions 3 \
  --replication-factor 1
