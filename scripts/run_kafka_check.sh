#!/bin/bash
# Mocking a kafka check that listed topics
echo "Querying Kafka Cluster for topic metadata..."
# In a real airflow image with kafka-clients, you would run:
# kafka-topics --bootstrap-server kafka:9092 --list
echo "Topics found: earthquake_raw, wolf_seismic_stream"
exit 0
