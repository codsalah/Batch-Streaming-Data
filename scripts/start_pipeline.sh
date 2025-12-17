#!/bin/bash
echo "Starting Seismic Data Pipeline Infrastructure..."
# Add any startup logic here if needed, e.g. checking if containers are up
# Check if internal services are reachable via nc (netcat)
# kafka:9092, spark-master:7077, postgres:5432
services="kafka:9092 spark-master:7077 postgres:5432"

for service_port in $services; do
    host=${service_port%:*}
    port=${service_port#*:}
    
    if nc -z -w 2 "$host" "$port"; then
        echo "Service $host:$port is reachable."
    else
        echo "Warning: Service $host:$port might not be reachable from this container."
    fi
done
echo "Pipeline startup initialized."
