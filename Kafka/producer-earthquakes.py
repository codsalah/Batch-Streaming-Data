#!/usr/bin/env python3
import json
import asyncio
import websockets
import os
from datetime import datetime
from kafka import KafkaProducer
# =======================================
from prometheus_client import start_http_server, Counter, Gauge
import time
# =======================================
from dotenv import load_dotenv

load_dotenv()

# ===== Prometheus Metrics Configuration =====
METRICS_PORT = int(os.getenv('EARTHQUAKE_METRICS_PORT', '8000'))

# ===== Kafka Configuration =====
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
if not KAFKA_BROKER:
    external_port = os.getenv('KAFKA_EXTERNAL_PORT', '9098')
    KAFKA_BROKER = f"localhost:{external_port}"
TOPIC = os.getenv('KAFKA_TOPIC_EARTHQUAKE', 'earthquake_raw')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8'),
    compression_type='gzip',
    enable_idempotence=True,
    acks='all',
    retries=5,   
    max_in_flight_requests_per_connection=1
)

# ===== WebSocket Configuration =====
SEISMIC_WS_URL = os.getenv('SEISMIC_WS_URL', 'wss://www.seismicportal.eu/standing_order/websocket')
PING_INTERVAL = int(os.getenv('SEISMIC_WS_PING_INTERVAL', '15'))

async def stream_seismic_data():
    print(f"Connecting to SeismicPortal WebSocket API...")
    
    # =====================================
    # counters
    messages_sent = Counter('earthquake_messages_total', 
                           'Total earthquake messages sent')
    websocket_connected = Gauge('earthquake_websocket_connected',
                               'WebSocket connection status (1=connected, 0=disconnected)')
    json_errors = Counter('earthquake_json_errors_total',
                         'Total JSON parsing errors')
    # =======================================
    
    while True:
        try:
            async with websockets.connect(SEISMIC_WS_URL, ping_interval=PING_INTERVAL) as websocket:
                print(f"Connected to SeismicPortal WebSocket API at {datetime.now()}")
                # =======================================
                websocket_connected.set(1)  # set to connected
                # =======================================
                
                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)

                        print(f"Received message: {data}")
                        
                        # Extract event_id for partition key
                        event_id = data.get('data', {}).get('properties', {}).get('unid', None)
                        
                        if event_id:
                            # Send to Kafka
                            producer.send(TOPIC, key=event_id, value=data)
                            # =======================================
                            messages_sent.inc()  # increment message counter
                            # =======================================
                            print(f"Sent event {event_id} to Kafka")
                        else:
                            print("No event_id found, skipping message")

                    except json.JSONDecodeError as e:
                        print(f"JSON parsing error: {e}")
                        # =======================================
                        json_errors.inc()  # record JSON error
                        # =======================================
                    except Exception as e:
                        print(f"Error processing message: {e}")
        except (websockets.exceptions.ConnectionClosed, OSError) as e:
            print(f"Connection lost: {e}. Retrying in 5 seconds...")
            # =======================================
            websocket_connected.set(0)  # set to disconnected
            # =======================================
            await asyncio.sleep(5)
        except Exception as e:
            print(f"Unexpected error: {e}. Retrying in 5 seconds...")
            # =======================================
            websocket_connected.set(0)  # set to disconnected
            # =======================================
            await asyncio.sleep(5)

def main():
    # =======================================
    # Starting Metrics Server
    start_http_server(METRICS_PORT)
    print(f"Earthquake Producer - Metrics server started on port {METRICS_PORT}")
    print(f"View metrics at: http://localhost:{METRICS_PORT}/metrics")
    # =======================================
    
    try:
        asyncio.run(stream_seismic_data())
    except KeyboardInterrupt:
        print("Producer stopped by user.")

if __name__ == "__main__":
    main()
