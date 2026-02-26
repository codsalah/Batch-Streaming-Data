#!/usr/bin/env python3
import json
import asyncio
import websockets
import os
import logging
import signal
from datetime import datetime
from kafka import KafkaProducer
from prometheus_client import start_http_server, Counter, Gauge
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("EarthquakeProducer")

load_dotenv()

# ===== Configuration Validation =====
def get_env_or_fail(var_name, default=None):
    value = os.getenv(var_name, default)
    if value is None:
        logger.error(f"Missing required environment variable: {var_name}")
        exit(1)
    return value

METRICS_PORT = int(os.getenv('EARTHQUAKE_METRICS_PORT', '8000'))
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
if not KAFKA_BROKER:
    external_port = os.getenv('KAFKA_EXTERNAL_PORT', '9098')
    KAFKA_BROKER = f"localhost:{external_port}"

TOPIC = get_env_or_fail('KAFKA_TOPIC_EARTHQUAKE', 'earthquake_raw')
SEISMIC_WS_URL = os.getenv('SEISMIC_WS_URL', 'wss://www.seismicportal.eu/standing_order/websocket')
PING_INTERVAL = int(os.getenv('SEISMIC_WS_PING_INTERVAL', '15'))

# ===== Prometheus Metrics =====
messages_sent = Counter('earthquake_messages_total', 'Total earthquake messages sent')
messages_failed = Counter('earthquake_messages_failed_total', 'Total earthquake messages failed to send')
validation_errors = Counter('earthquake_validation_errors_total', 'Total data validation errors')
websocket_connected = Gauge('earthquake_websocket_connected', 'WebSocket connection status (1=connected, 0=disconnected)')
json_errors = Counter('earthquake_json_errors_total', 'Total JSON parsing errors')

# Initialize Kafka Producer
try:
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
    logger.info(f"Kafka Producer initialized for broker: {KAFKA_BROKER}")
except Exception as e:
    logger.error(f"Failed to initialize Kafka Producer: {e}")
    exit(1)

def validate_earthquake_data(data):
    """
    Basic quality check for earthquake data.
    Returns True if valid, False otherwise.
    """
    try:
        if not isinstance(data, dict) or 'data' not in data:
            return False
        
        properties = data.get('data', {}).get('properties', {})
        if not properties:
            return False
        
        # Check for mandatory fields
        mandatory_fields = ['unid', 'time', 'lat', 'lon', 'mag']
        for field in mandatory_fields:
            if field not in properties or properties[field] is None:
                logger.warning(f"Missing mandatory field '{field}' in message: {properties.get('unid', 'unknown')}")
                return False
        
        # Range checks
        lat = float(properties['lat'])
        lon = float(properties['lon'])
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            logger.warning(f"Coordinates out of range: lat={lat}, lon={lon} for unid={properties['unid']}")
            return False
            
        return True
    except (ValueError, TypeError) as e:
        logger.warning(f"Validation error: {e}")
        return False

async def stream_seismic_data():
    logger.info(f"Connecting to SeismicPortal WebSocket API...")
    
    while True:
        try:
            async with websockets.connect(SEISMIC_WS_URL, ping_interval=PING_INTERVAL) as websocket:
                logger.info(f"Connected to SeismicPortal WebSocket API")
                websocket_connected.set(1)
                
                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)

                        if validate_earthquake_data(data):
                            event_id = data['data']['properties']['unid']
                            
                            # Send to Kafka
                            future = producer.send(TOPIC, key=event_id, value=data)
                            # Wait for send to complete to ensure reliability if needed
                            # future.get(timeout=10) 
                            
                            messages_sent.inc()
                            logger.info(f"Sent event {event_id} to Kafka")
                        else:
                            validation_errors.inc()
                            logger.debug("Validation failed for message, skipping")

                    except json.JSONDecodeError as e:
                        logger.error(f"JSON parsing error: {e}")
                        json_errors.inc()
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        messages_failed.inc()

        except (websockets.exceptions.ConnectionClosed, OSError) as e:
            logger.warning(f"Connection lost: {e}. Retrying in 5 seconds...")
            websocket_connected.set(0)
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error: {e}. Retrying in 5 seconds...")
            websocket_connected.set(0)
            await asyncio.sleep(5)

def main():
    # Starting Metrics Server
    start_http_server(METRICS_PORT)
    logger.info(f"Earthquake Producer - Metrics server started on port {METRICS_PORT}")
    
    # Handle termination signals
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(shutdown(sig)))

    try:
        loop.run_until_complete(stream_seismic_data())
    except asyncio.CancelledError:
        pass

async def shutdown(sig):
    logger.info(f"Received exit signal {sig.name}...")
    websocket_connected.set(0)
    producer.flush()
    producer.close()
    logger.info("Kafka Producer flushed and closed.")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [t.cancel() for t in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop = asyncio.get_event_loop()
    loop.stop()

if __name__ == "__main__":
    main()
