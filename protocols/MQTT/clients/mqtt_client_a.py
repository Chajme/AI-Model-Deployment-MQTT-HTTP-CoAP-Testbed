import paho.mqtt.client as mqtt
import time
import os
import json
import math

from output.write_csv import write_to_file_mqtt

BROKER = "mosquitto-broker"
TOPIC_CTRL = "file/control"
TOPIC_DATA = "file/data"
DATA_DIR = "/app/data"

# 256 KB chunk size is generally safe for Mosquitto defaults
CHUNK_SIZE = 256 * 1024

client = mqtt.Client()
client.connect(BROKER, 1883, 60)
client.loop_start()

def calculate_total_chunks(filepath):
    file_size = os.path.getsize(filepath)
    total_chunks = math.ceil(file_size / CHUNK_SIZE)

    return file_size, total_chunks

def send_metadata(filename, total_chunks, qos_level):
    # 1. Send Metadata (so the receiver knows what to expect)
    metadata = {"filename": filename, "total_chunks": total_chunks}
    client.publish(TOPIC_CTRL, json.dumps(metadata), qos=qos_level)

def send_chunks(filepath, total_chunks):
    # 2. Send the Chunks
    with open(filepath, "rb") as f:
        for chunk_num in range(total_chunks):
            chunk = f.read(CHUNK_SIZE)

            # Using QoS 1 ensures the broker confirms receipt of the chunk
            client.publish(TOPIC_DATA, bytearray(chunk), qos=qos_level)

            # Simple progress print
            if chunk_num % 10 == 0 or chunk_num == total_chunks - 1:
                print(f"Sent chunk {chunk_num + 1}/{total_chunks}")

            # Tiny sleep to avoid completely flooding the broker
            time.sleep(0.01)

def load_files():
    if not os.path.exists(DATA_DIR):
        print(f"Error: Directory '{DATA_DIR}' does not exist.")

    # Grab all files in the directory
    files = [
        f for f in os.listdir(DATA_DIR)
        if os.path.isfile(os.path.join(DATA_DIR, f)) and f.endswith(".bin")
    ]

    return files

def send_file(filename, qos_level):
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        print(f"File {filepath} not found.")
        return

    file_size, total_chunks = calculate_total_chunks(filepath)

    print(f"\n--- Starting transfer: {filename} ({file_size / 1024 / 1024:.2f} MB) ---")

    start_time = time.time()

    send_metadata(filename, total_chunks, qos_level)
    time.sleep(0.5)  # Give the receiver a moment to open the file

    send_chunks(filepath, total_chunks)

    end_time = time.time()
    duration = end_time - start_time

    measurements = [
        {'protocol': 'mqtt',
         'qos': qos_level,
         'side': 'sender',
         'file_size': file_size / (1024 * 1024),
         'sender_duration': f"{duration:.2f}",
         'receiver_duration': "X"}
    ]
    write_to_file_mqtt(measurements)

    print("Finished sending file.")
    print(f"Sender Time: {duration:.2f} seconds ({(file_size / (1024 * 1024)) / duration:.2f} MB/s)")

def qos_levels_loop(files):
    for qos_level in range (0, 3):
        for filename in files:
            send_file(filename, qos_level)


if __name__ == "__main__":
    time.sleep(5)  # Wait for broker and receiver to spin up

    files = load_files()
    qos_levels_loop(files)

    time.sleep(10)
    client.loop_stop()