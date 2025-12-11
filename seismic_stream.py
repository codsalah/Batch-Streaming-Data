#!/usr/bin/env python3
"""
Real-time Seismic Data Stream Explorer
Connects to EMSC SeismicPortal WebSocket API to receive earthquake notifications
Saves data to CSV file with timestamps
"""

import json
import sys
import csv
import os
from datetime import datetime
import asyncio
import websockets


# WebSocket endpoint for real-time seismic events
SEISMIC_WS_URL = 'wss://www.seismicportal.eu/standing_order/websocket'
PING_INTERVAL = 15
CSV_FILENAME = 'seismic_events.csv'

# CSV Headers
CSV_HEADERS = [
    'received_timestamp',
    'event_time',
    'action',
    'magnitude',
    'magnitude_type',
    'region',
    'depth_km',
    'latitude',
    'longitude',
    'authority',
    'event_id'
]


def format_event(data):
    """Format earthquake event data for terminal display"""
    try:
        action = data.get('action', 'UNKNOWN')
        properties = data.get('data', {}).get('properties', {})
        
        # Extract key information
        mag = properties.get('mag', 'N/A')
        mag_type = properties.get('magtype', 'N/A')
        region = properties.get('flynn_region', 'Unknown Region')
        depth = properties.get('depth', 'N/A')
        time = properties.get('time', 'N/A')
        auth = properties.get('auth', 'N/A')
        unid = properties.get('unid', 'N/A')
        lat = properties.get('lat', 'N/A')
        lon = properties.get('lon', 'N/A')
        
        # Create formatted output
        separator = "=" * 80
        output = f"\n{separator}\n"
        output += f"[{action}] Earthquake Event\n"
        output += f"{separator}\n"
        output += f"  Time:      {time}\n"
        output += f"  Magnitude: {mag_type} {mag}\n"
        output += f"  Location:  {region}\n"
        output += f"  Depth:     {depth} km\n"
        output += f"  Coords:    {lat}°N, {lon}°E\n"
        output += f"  Authority: {auth}\n"
        output += f"  Event ID:  {unid}\n"
        output += f"{separator}\n"
        
        return output
    except Exception as e:
        return f"Error formatting event: {e}\n"


def write_to_csv(data, csv_filename=CSV_FILENAME):
    """Write earthquake event data to CSV file"""
    try:
        action = data.get('action', 'UNKNOWN')
        properties = data.get('data', {}).get('properties', {})
        
        # Extract data
        received_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        event_time = properties.get('time', '')
        mag = properties.get('mag', '')
        mag_type = properties.get('magtype', '')
        region = properties.get('flynn_region', '')
        depth = properties.get('depth', '')
        lat = properties.get('lat', '')
        lon = properties.get('lon', '')
        auth = properties.get('auth', '')
        unid = properties.get('unid', '')
        
        # Prepare row data
        row = [
            received_time,
            event_time,
            action,
            mag,
            mag_type,
            region,
            depth,
            lat,
            lon,
            auth,
            unid
        ]
        
        # Check if file exists to determine if we need to write headers
        file_exists = os.path.isfile(csv_filename)
        
        # Append to CSV file
        with open(csv_filename, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header if file is new
            if not file_exists:
                writer.writerow(CSV_HEADERS)
                print(f"Created new CSV file: {csv_filename}")
            
            # Write data row
            writer.writerow(row)
            print(f"Saved to CSV: {csv_filename}")
        
        return True
    except Exception as e:
        print(f"Error writing to CSV: {e}")
        return False



async def stream_seismic_data():
    """Connect to WebSocket and stream earthquake data"""
    print(f"Connecting to SeismicPortal WebSocket API...")
    print(f"Endpoint: {SEISMIC_WS_URL}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"\n{'='*80}")
    print("Waiting for earthquake notifications... (Press Ctrl+C to stop)")
    print(f"{'='*80}\n")
    
    try:
        async with websockets.connect(
            SEISMIC_WS_URL,
            ping_interval=PING_INTERVAL
        ) as websocket:
            print("Connected. Listening for events...\n")
            
            while True:
                try:
                    # Receive message from WebSocket
                    message = await websocket.recv()
                    
                    # Parse JSON data
                    data = json.loads(message)
                    
                    # Write to CSV file
                    write_to_csv(data)
                    
                    # Print formatted event
                    print(format_event(data))
                    
                    # Print raw JSON
                    print("Raw JSON Data:")
                    print(json.dumps(data, indent=2))
                    print("\n")
                    
                except json.JSONDecodeError as e:
                    print(f"JSON parsing error: {e}")
                    print(f"Raw message: {message}\n")
                except Exception as e:
                    print(f"Error processing message: {e}\n")
                    
    except websockets.exceptions.WebSocketException as e:
        print(f"WebSocket error: {e}")
        return 1
    except KeyboardInterrupt:
        print("\n\nStopped by user")
        return 0
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1


def main():
    """Main entry point"""
    try:
        # Run the async event loop
        return asyncio.run(stream_seismic_data())
    except KeyboardInterrupt:
        print("\n\nStopped by user")
        return 0


if __name__ == "__main__":
    sys.exit(main())
