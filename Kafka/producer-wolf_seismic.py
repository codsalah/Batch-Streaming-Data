import asyncio
import json
import os
import websockets
import logging
import signal
from kafka import KafkaProducer
from dotenv import load_dotenv
from prometheus_client import start_http_server, Counter, Gauge

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("WolfProducer")

load_dotenv()

# ===== Configuration Validation =====
def get_env_or_fail(var_name, default=None):
    value = os.getenv(var_name, default)
    if value is None:
        logger.error(f"Missing required environment variable: {var_name}")
        exit(1)
    return value

METRICS_PORT = int(os.getenv('WOLF_METRICS_PORT', '8001'))
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
if not KAFKA_BROKER:
    external_port = os.getenv('KAFKA_EXTERNAL_PORT', '9098')
    KAFKA_BROKER = f"localhost:{external_port}"

TOPIC = get_env_or_fail('KAFKA_TOPIC_WOLF', 'wolf_seismic_stream')
WOLF_URL = os.getenv('WOLF_URL', 'wss://seisjs.wolfx.jp/all_seis')

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        linger_ms=5,
        retries=5,
        enable_idempotence=True
    )
    logger.info(f"Kafka Producer initialized for broker: {KAFKA_BROKER}")
except Exception as e:
    logger.error(f"Failed to initialize Kafka Producer: {e}")
    exit(1)

# ===== Prometheus Metrics =====
messages_sent = Counter('wolf_messages_total', 'Total wolf seismic messages sent')
messages_failed = Counter('wolf_messages_failed_total', 'Total wolf seismic messages failed to send')
validation_errors = Counter('wolf_validation_errors_total', 'Total data validation errors')
websocket_connected = Gauge('wolf_websocket_connected', 'WebSocket connection status (1=connected, 0=disconnected)')
connection_errors = Counter('wolf_connection_errors_total', 'Total connection errors')

def validate_wolf_data(data):
    """
    Basic quality check for wolf seismic data.
    """
    try:
        if not isinstance(data, dict):
            return False
        
        # Check heartbeat
        if data.get('type') == 'heartbeat':
            return False

        mandatory_fields = ['latitude', 'longitude', 'Station']
        for field in mandatory_fields:
            if field not in data or data[field] is None:
                logger.warning(f"Missing mandatory field '{field}' in message: {data}. Full message: {data}")
                return False
        
        # Range checks
        lat = float(data['latitude'])
        lon = float(data['longitude'])
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            logger.warning(f"Coordinates out of range: lat={lat}, lon={lon} for Station={data['Station']}")
            return False
            
        return True
    except (ValueError, TypeError) as e:
        logger.warning(f"Validation error: {e}")
        return False

async def stream_seismic_data():
    logger.info(f"Connecting to Wolf API at {WOLF_URL}...")
    
    while True:
        try:
            async with websockets.connect(WOLF_URL) as websocket:
                logger.info("Connected to Wolf API")
                websocket_connected.set(1)   
                
                while True:
                    try:
                        message = await websocket.recv()
                        
                        try:
                            data = json.loads(message)
                        except json.JSONDecodeError:
                            logger.warning(f"Received non-JSON message: {message[:100]}")
                            continue

                            logger.debug(f"Skipping message (heartbeat or validation failed): {str(data)[:200]}")
                            if data.get('type') != 'heartbeat':
                                validation_errors.inc()
                                logger.warning(f"Validation failed for message: {str(data)[:200]}")

                    except websockets.exceptions.ConnectionClosed:
                        logger.warning("WebSocket connection closed")
                        websocket_connected.set(0)
                        break
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        messages_failed.inc()
                        connection_errors.inc()
                        
        except Exception as e:
            logger.error(f"Connection error: {e}. Retrying in 5 seconds...")
            websocket_connected.set(0)
            connection_errors.inc()
            await asyncio.sleep(5)

async def shutdown(sig, loop):
    logger.info(f"Received exit signal {sig.name}...")
    websocket_connected.set(0)
    producer.flush()
    producer.close()
    logger.info("Kafka Producer flushed and closed.")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [t.cancel() for t in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

if __name__ == "__main__":
    # Starting Metrics Server
    start_http_server(METRICS_PORT)
    logger.info(f"Wolf Producer - Metrics server started on port {METRICS_PORT}")
    
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(shutdown(sig, loop)))

    try:
        loop.run_until_complete(stream_seismic_data())
    except asyncio.CancelledError:
        pass
