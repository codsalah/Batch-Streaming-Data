import asyncio
import json
import os
import websockets
from kafka import KafkaProducer
from dotenv import load_dotenv
# =======================================
from prometheus_client import start_http_server, Counter, Gauge
import time
# =======================================

load_dotenv()

# ===== Prometheus Metrics Configuration =====
METRICS_PORT = int(os.getenv('WOLF_METRICS_PORT', '8001'))

# ===== Kafka Configuration =====
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
if not KAFKA_BROKER:
    external_port = os.getenv('KAFKA_EXTERNAL_PORT', '9098')
    KAFKA_BROKER = f"localhost:{external_port}"

TOPIC = os.getenv('KAFKA_TOPIC_WOLF', 'wolf_seismic_stream')
WOLF_URL = os.getenv('WOLF_URL', 'wss://seisjs.wolfx.jp/all_seis')

# Initialize Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        linger_ms=5,
        retries=5
    )
except Exception as e:
    print(f"Failed to initialize Kafka Producer: {e}")
    exit(1)

async def stream_seismic_data():
    print(f"Connecting to Wolf API at {WOLF_URL}...")
    
    # =====================================
    # counters
    messages_sent = Counter('wolf_messages_total', 
                           'Total wolf seismic messages sent')
    websocket_connected = Gauge('wolf_websocket_connected',
                               'WebSocket connection status (1=connected, 0=disconnected)')
    connection_errors = Counter('wolf_connection_errors_total',
                               'Total connection errors')
    # =======================================
    
    while True:
        try:
            async with websockets.connect(WOLF_URL) as websocket:
                print("Connected to Wolf API")
                # =======================================
                websocket_connected.set(1)   
                # =======================================
                
                while True:
                    try:
                        message = await websocket.recv()
                        
                        # Handle text heartbeat if necessary, but usually we just process JSON
                        try:
                            data = json.loads(message)
                        except json.JSONDecodeError:
                            print(f"Received non-JSON message: {message}")
                            continue

                        # Check heartbeat
                        if data.get('type') == 'heartbeat':
                            # print("Heartbeat received") # Optional logging
                            continue

                        # Send to Kafka (using 'Station' as key)
                        key = None
                        if 'Station' in data:
                             key = str(data['Station'])
                        
                        producer.send(TOPIC, key=key, value=data)
                        # =======================================
                        messages_sent.inc()  # increment message counter
                        # =======================================
                        print(f"Sent message to Kafka: {str(data)[:100]}...")

                    except websockets.exceptions.ConnectionClosed:
                        print("WebSocket connection closed")
                        # =======================================
                        websocket_connected.set(0)  # set to disconnected
                        # =======================================
                        break
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        # =======================================
                        connection_errors.inc()   
                        # =======================================
                        
        except Exception as e:
            print(f"Connection error: {e}. Retrying in 5 seconds...")
            # =======================================
            websocket_connected.set(0)  # set to disconnected
            connection_errors.inc()     # inc error counter
            # =======================================
            await asyncio.sleep(5)

if __name__ == "__main__":
    # =======================================
    # Starting Metrics Server
    start_http_server(METRICS_PORT)
    print(f"Wolf Producer - Metrics server started on port {METRICS_PORT}")
    print(f"View metrics at: http://localhost:{METRICS_PORT}/metrics")
    # =======================================
    
    try:
        asyncio.run(stream_seismic_data())
    except KeyboardInterrupt:
        print("Producer stopped by user")
