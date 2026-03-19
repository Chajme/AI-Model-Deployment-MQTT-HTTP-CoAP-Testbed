# import time
# import paho.mqtt.client as mqtt
#
# BROKER = "mosquitto-broker"
# TOPIC = "paho/temperature"
#
# client = mqtt.Client()
#
# client.connect(BROKER, 1883, 60)
#
# client.loop_start()
#
# message = 0
#
# while True:
#     print(f"Sending {message}")
#     client.publish(TOPIC, message)
#     message += 1
#     time.sleep(1)

import paho.mqtt.client as mqtt
import time
import os
import json
import math

BROKER = "mosquitto-broker"
TOPIC_CTRL = "file/control"
TOPIC_DATA = "file/data"
DATA_DIR = "/app/data"

# 256 KB chunk size is generally safe for Mosquitto defaults
CHUNK_SIZE = 256 * 1024

client = mqtt.Client()
client.connect(BROKER, 1883, 60)
client.loop_start()


def send_file(filename):
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        print(f"File {filepath} not found.")
        return

    file_size = os.path.getsize(filepath)
    total_chunks = math.ceil(file_size / CHUNK_SIZE)

    print(f"\n--- Starting transfer: {filename} ({file_size / 1024 / 1024:.2f} MB) ---")

    start_time = time.time()

    # 1. Send Metadata (so the receiver knows what to expect)
    metadata = {"filename": filename, "total_chunks": total_chunks}
    client.publish(TOPIC_CTRL, json.dumps(metadata), qos=1)
    time.sleep(0.5)  # Give the receiver a moment to open the file

    # 2. Send the Chunks
    with open(filepath, "rb") as f:
        for chunk_num in range(total_chunks):
            chunk = f.read(CHUNK_SIZE)

            # Using QoS 1 ensures the broker confirms receipt of the chunk
            client.publish(TOPIC_DATA, bytearray(chunk), qos=1)

            # Simple progress print
            if chunk_num % 10 == 0 or chunk_num == total_chunks - 1:
                print(f"Sent chunk {chunk_num + 1}/{total_chunks}")

            # Tiny sleep to avoid completely flooding the broker
            time.sleep(0.01)

    end_time = time.time()
    duration = end_time - start_time

    print("Finished sending file.")
    print(f"Sender Time: {duration:.2f} seconds ({(file_size / 1024 / 1024) / duration:.2f} MB/s)")


if __name__ == "__main__":
    time.sleep(5)  # Wait for broker and receiver to spin up

    if not os.path.exists(DATA_DIR):
        print(f"Error: Directory '{DATA_DIR}' does not exist.")

    # Grab all files in the directory
    files = [
        f for f in os.listdir(DATA_DIR)
        if os.path.isfile(os.path.join(DATA_DIR, f)) and f.endswith(".bin")
    ]

    for filename in files:
        # Example: Send the 5MB file from your screenshot
        send_file(filename)

    time.sleep(10)
    client.loop_stop()