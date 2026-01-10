#!/usr/bin/env python3
import json
import asyncio
import os
import time
import csv
from kafka import KafkaProducer
from dotenv import load_dotenv
from prometheus_client import start_http_server, Counter, Gauge

# Load environment variables
load_dotenv()

# ===== Prometheus Metrics Configuration =====
METRICS_PORT = int(os.getenv('AIRPORT_METRICS_PORT', '8002'))

# ===== Kafka Configuration =====
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
if not KAFKA_BROKER:
    external_port = os.getenv('KAFKA_EXTERNAL_PORT', '9098')
    KAFKA_BROKER = f"localhost:{external_port}"

TOPIC = os.getenv('KAFKA_TOPIC_AIRPORT', 'airport_stream')

# ===== CSV Configuration =====
CSV_PATH = os.path.abspath("data/airports.csv")
# 12 hours in seconds = 12 * 60 * 60
POLL_INTERVAL = 12 * 60 * 60 

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: str(v).encode('utf-8'),
        compression_type='gzip',
        enable_idempotence=True,
        acks='all',
        retries=5,
        max_in_flight_requests_per_connection=1
    )
except Exception as e:
    print(f"Failed to initialize Kafka Producer: {e}")
    exit(1)

async def stream_airport_data():
    print(f"Starting Airport Data Producer (CSV Mode)...")
    print(f"Reading from: {CSV_PATH}")
    
    # counters and gauges (prometheus metrics)
    messages_sent = Counter('airport_messages_total', 'Total airport messages sent')
    csv_status = Gauge('airport_csv_status', 'CSV read status (1=success, 0=error)')
    
    while True:
        print(f"Starting new CSV processing cycle at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
        
        try:
            if not os.path.exists(CSV_PATH):
                print(f"Error: CSV file not found at {CSV_PATH}")
                csv_status.set(0)
            else:
                counter = 0
                with open(CSV_PATH, mode='r', encoding='utf-8') as f:
                    # Using DictReader to handle columns by name
                    reader = csv.DictReader(f)
                    for row in reader:
                        # Requirements: ICAO, name, lat, lon
                        # CSV Header: id,ident,icao,iata,gps_code,local_code,name,latitude,longitude,elevation,continent,country,region,municipality,scheduled_service,web_url,wikipedia_url,keywords,timezone,airport_type
                        
                        try:
                            # Required fields
                            row['latitude'] = float(row.get('latitude'))
                            row['longitude'] = float(row.get('longitude'))
                            
                            # Optional numeric fields
                            if row.get('elevation'):
                                try:
                                    row['elevation'] = float(row['elevation'])
                                except ValueError:
                                    row['elevation'] = None
                            
                            if row.get('id'):
                                try:
                                    row['id'] = int(row['id'])
                                except ValueError:
                                    pass # Keep as string if it's not an int
                                    
                        except (ValueError, TypeError):
                            # If latitude or longitude are missing/invalid, skip this record
                            continue

                        icao = row.get('icao') or row.get('ident')
                        if not icao:
                            continue

                        row["last_update"] = time.strftime('%Y-%m-%dT%H:%M:%S')
                        producer.send(TOPIC, key=icao, value=row)
                        messages_sent.inc()
                        counter += 1
                        
                        # Optional: yield to event loop periodically if file is huge
                        if counter % 1000 == 0:
                            await asyncio.sleep(0.01)
                
                producer.flush()
                csv_status.set(1)
                print(f"Cycle complete. Sent {counter} airports to Kafka.")
        
        except Exception as e:
            print(f"Error processing CSV: {e}")
            csv_status.set(0)
        
        print(f"Waiting {POLL_INTERVAL}s for next cycle...")
        await asyncio.sleep(POLL_INTERVAL)

def main():
    # Starting Metrics Server
    start_http_server(METRICS_PORT)
    print(f"Airport Producer - Metrics server started on port {METRICS_PORT}")
    
    try:
        asyncio.run(stream_airport_data())
    except KeyboardInterrupt:
        print("Producer stopped by user.")

if __name__ == "__main__":
    main()
