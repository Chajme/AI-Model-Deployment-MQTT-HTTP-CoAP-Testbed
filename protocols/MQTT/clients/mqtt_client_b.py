import paho.mqtt.client as mqtt
import json
import os
import time

from output.integrity_checker import compute_sha256_file
from output.write_csv import write_to_file_mqtt

BROKER = "mosquitto-broker"
TOPIC_CTRL = "file/control"
TOPIC_DATA = "file/data"
OUTPUT_DIR = "/app/output"

# Global state to keep track of the incoming file
current_file_handle = None
expected_chunks = 0
received_chunks = 0
current_filename = ""
received_bytes = 0
first_chunk_received = False

# Measurements
start_latency = 0
metadata_arrival_time = 0
transfer_start_time = 0

expected_checksum = None

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

def transfer_completed_handler():
    global start_latency, transfer_start_time, expected_checksum

    transfer_duration = time.perf_counter() - transfer_start_time
    file_size_mb = received_bytes / (1024 * 1024)

    filepath = os.path.join(OUTPUT_DIR, current_filename)

    actual_checksum = compute_sha256_file(filepath)
    integrity_ok = (expected_checksum == actual_checksum)

    if integrity_ok:
        print(f"File {current_filename} OK (checksum match)")
    else:
        print(f"File {current_filename} CORRUPTED (checksum mismatch)")
    print(f"Latency (Start Lag): {start_latency:.4f}s")
    speed = (file_size_mb / transfer_duration) if transfer_duration > 0 else 0
    print(f"Receiver Time: {transfer_duration:.2f} seconds ({speed:.2f} MB/s)")

    measurements = [
        {'protocol': 'mqtt',
         'qos': "X",
         'side': 'receiver',
         'file_size': file_size_mb,
         'sender_duration': "X",
         'receiver_duration': f"{transfer_duration:.2f}",
         'latency': f"{start_latency:.4f}",
         'payload_overhead': "X",
         'integrity_ok': integrity_ok
         }
    ]
    write_to_file_mqtt(measurements)

def on_connect(client, userdata, flags, rc):
    print("Connected to broker. Listening for files...")
    client.subscribe(TOPIC_CTRL, qos=2)
    client.subscribe(TOPIC_DATA, qos=2)

def on_message(client, userdata, msg):
    global current_file_handle, expected_chunks, received_chunks, \
        current_filename, start_latency, received_bytes, metadata_arrival_time, \
        first_chunk_received, transfer_start_time, expected_checksum

    # Handle Metadata Message
    if msg.topic == TOPIC_CTRL:
        metadata_arrival_time = time.perf_counter()
        first_chunk_received = False

        metadata = json.loads(msg.payload.decode())
        current_filename = metadata["filename"]
        expected_chunks = metadata["total_chunks"]
        received_chunks = 0
        received_bytes = 0

        start_latency = 0
        transfer_start_time = 0

        expected_checksum = metadata.get("checksum")

        filepath = os.path.join(OUTPUT_DIR, current_filename)
        print(f"\nIncoming file: {current_filename} ({expected_chunks} chunks). Opening {filepath}...")

        # Open file in 'wb' (write binary) mode
        if current_file_handle and not current_file_handle.closed:
            current_file_handle.close()
        current_file_handle = open(filepath, "wb")

    # Handle Raw Binary Chunk Message
    elif msg.topic == TOPIC_DATA and current_file_handle is not None:
        if not first_chunk_received:
            # Calculate the time since we got the metadata
            start_latency = time.perf_counter() - metadata_arrival_time
            transfer_start_time = time.perf_counter()
            first_chunk_received = True
            print(f"First chunk arrived. Latency: {start_latency:.4f}s")

        current_file_handle.write(msg.payload)
        received_chunks += 1
        received_bytes += len(msg.payload)

        if received_chunks % 10 == 0 or received_chunks == expected_chunks:
            print(f"Received chunk {received_chunks}/{expected_chunks}")

        # If we have received all chunks, close the file
        if received_chunks == expected_chunks:
            transfer_completed_handler()

            received_bytes = 0
            current_file_handle.close()
            current_file_handle = None


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, 1883, 300)
client.loop_forever()