import asyncio
import json
import os
import websockets
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# ===== Kafka Configuration =====
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
if not KAFKA_BROKER:
    # If KAFKA_BROKER is not set, try KAFKA_EXTERNAL_PORT (host:port assumption)
    external_port = os.getenv('KAFKA_EXTERNAL_PORT', '9093')
    KAFKA_BROKER = f"localhost:{external_port}"

TOPIC = os.getenv('KAFKA_TOPIC', 'real_earthquake')
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
    while True:
        try:
            async with websockets.connect(WOLF_URL) as websocket:
                print("Connected to Wolf API")
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
                        print(f"Sent message to Kafka: {str(data)[:100]}...")

                    except websockets.exceptions.ConnectionClosed:
                        print("WebSocket connection closed")
                        break
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        
        except Exception as e:
            print(f"Connection error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(stream_seismic_data())
    except KeyboardInterrupt:
        print("Producer stopped by user")
