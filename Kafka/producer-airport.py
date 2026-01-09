#!/usr/bin/env python3
import json
import asyncio
import os
import time
import requests
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

# ===== Airport API Configuration =====
AIRPORT_BASE_URL = "https://airport-web.appspot.com/_ah/api/airportsapi/v1/airports"
POLL_INTERVAL = int(os.getenv('AIRPORT_POLL_INTERVAL', '60'))

# Coordinates mapping for German airports (Lat, Lon)
# Earthquake data is in the format {"lat": lat, "lon": lon} same as airport data
AIRPORT_COORDINATES = {
    "EDDF": (50.033, 8.570),  # Frankfurt
    "EDDM": (48.353, 11.775), # Munich
    "EDDL": (51.289, 6.766),  # Dusseldorf
    "EDDB": (52.362, 13.501), # Berlin
    "EDDH": (53.630, 9.988),  # Hamburg
    "EDDS": (48.689, 9.221),  # Stuttgart
    "EDDN": (49.498, 11.078), # Nuremberg
    "EDDP": (51.423, 12.236), # Leipzig
    "EDDC": (51.132, 13.767), # Dresden
    "EDDK": (50.865, 7.142),  # Cologne
    "EDDW": (53.047, 8.786),  # Bremen
    "EDLW": (51.518, 7.612),  # Dortmund
    "EDFH": (49.948, 7.263),  # Frankfurt-Hahn
    "EDDE": (50.979, 10.958), # Erfurt
    "EDJA": (47.988, 10.239), # Memmingen
    "EDNY": (47.671, 9.511),  # Friedrichshafen
    "EDDR": (49.214, 7.109),  # Saarbrücken
    "EDGS": (50.707, 8.082),  # Siegerland
    "EDKA": (48.779, 8.080)   # Karlsruhe
}

ICAO_CODES = list(AIRPORT_COORDINATES.keys())

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
    print(f"Starting Airport Data Producer...")
    print(f"Polling {len(ICAO_CODES)} airports from {AIRPORT_BASE_URL}")
    
    # counters and gauges (prometheus metrics)
    messages_sent = Counter('airport_messages_total', 'Total airport messages sent')
    api_status = Gauge('airport_api_status', 'API connection status (1=connected, 0=error)')
    api_errors = Counter('airport_api_errors_total', 'Total API errors encountered')
    
    while True:
        print(f"Starting new polling cycle at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
        
        for icao in ICAO_CODES:
            url = f"{AIRPORT_BASE_URL}/{icao}"
            # Try to fetch data from API (set api_status to 0 if error)
            try:
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    api_status.set(1)
                    data = response.json()
                    
                    if data and 'ICAO' in data:
                        # Add coordinates for spatial comparison
                        coords = AIRPORT_COORDINATES.get(icao)
                        if coords:
                            data['lat'] = coords[0]
                            data['lon'] = coords[1]
                        
                        producer.send(TOPIC, key=icao, value=data)
                        messages_sent.inc()
                else:
                    print(f"Failed to fetch data for {icao}: {response.status_code}")
                    api_status.set(0)
                    api_errors.inc()
                
                # Small delay between requests to be nice to the API
                await asyncio.sleep(1)
            
            except Exception as e:
                print(f"Error fetching data for {icao}: {e}")
                api_status.set(0)
                api_errors.inc()
        
        producer.flush()
        print(f"Polling cycle complete. Next cycle in {POLL_INTERVAL}s.")
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
