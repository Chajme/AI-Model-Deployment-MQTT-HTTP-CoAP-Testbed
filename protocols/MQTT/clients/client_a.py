import paho.mqtt.client as mqtt
import hashlib
import time
import os
import json
import math
import threading

from output.integrity_checker import compute_sha256_file
from output.resource_monitor import ResourceMonitor
from output.write_csv import write_to_file_mqtt

BROKER = "mosquitto-broker"
TOPIC_CTRL = "file/control"
TOPIC_DATA = "file/data"
DATA_DIR = "/app/data"


# CHUNK_SIZE = 256 * 1024
CHUNK_SIZE = 512 * 1024
# CHUNK_SIZE = 1024 * 1024

# MQTT control packet sizes (bytes) for broker-to-client ACK traffic.
# These are fixed-size packets defined by the MQTT 3.1.1 spec:
#   PUBACK   (QoS 1): 2-byte fixed header + 2-byte packet ID = 4 bytes
#   PUBREC   (QoS 2): 2-byte fixed header + 2-byte packet ID = 4 bytes
#   PUBCOMP  (QoS 2): 2-byte fixed header + 2-byte packet ID = 4 bytes
# PUBREL is sent client→broker so it is already counted on the sender side
# via the PUBLISH framing; it is NOT a broker-to-client packet.
_PUBACK_BYTES = 4    # QoS 1: broker → sender
_PUBREC_BYTES = 4    # QoS 2: broker → sender
_PUBCOMP_BYTES = 4   # QoS 2: broker → sender


def mqtt_publish_packet_bytes(topic: str, payload_len: int, qos: int) -> int:
    """MQTT 3.1.1 PUBLISH: fixed header + remaining-length encoding + variable header + payload."""
    topic_b = topic.encode("utf-8")
    var_header = 2 + len(topic_b) + (2 if qos > 0 else 0)
    remaining_length = var_header + payload_len
    rl_bytes = 0
    x = remaining_length
    while True:
        rl_bytes += 1
        x //= 128
        if x == 0:
            break
    return 1 + rl_bytes + remaining_length


def mqtt_ack_overhead_bytes(num_packets: int, qos_level: int) -> int:
    """
    Broker-to-client ACK traffic generated for a given number of published packets.

    QoS 0: no ACKs.
    QoS 1: one PUBACK per packet.
    QoS 2: one PUBREC + one PUBCOMP per packet (PUBREL is client→broker,
            accounted for separately if needed, but omitted here as it is
            a sender-side packet, not broker overhead back to the sender).
    """
    if qos_level == 0:
        return 0
    elif qos_level == 1:
        return num_packets * _PUBACK_BYTES
    else:  # QoS 2
        return num_packets * (_PUBREC_BYTES + _PUBCOMP_BYTES)


def mqtt_transfer_overhead_bytes(
    topic_ctrl: str,
    topic_data: str,
    metadata_payload_len: int,
    chunk_payload_lens: list[int],
    qos_level: int,
    file_size_bytes: int,
) -> float:
    """
    Total wire overhead = all PUBLISH framing (sender→broker)
                        + all ACK packets (broker→sender)
                        - raw file payload bytes.
    """
    # Sender → broker: PUBLISH framing for metadata + all chunks
    total_wire = mqtt_publish_packet_bytes(topic_ctrl, metadata_payload_len, qos_level)
    for plen in chunk_payload_lens:
        total_wire += mqtt_publish_packet_bytes(topic_data, plen, qos_level)

    # Broker → sender: ACK packets (QoS-dependent)
    # +1 for the metadata PUBLISH
    num_published = len(chunk_payload_lens) + 1
    total_wire += mqtt_ack_overhead_bytes(num_published, qos_level)

    return float(total_wire - file_size_bytes)


metadata_ack_event = threading.Event()
metadata_mid = None
metadata_sent_time = 0
ack_latency = 0


def on_publish(client, userdata, mid):
    global metadata_mid, ack_latency
    if mid == metadata_mid:
        ack_latency = time.perf_counter() - metadata_sent_time
        print(f"Metadata ACK received in {ack_latency:.4f}s")
        metadata_ack_event.set()


client = mqtt.Client()
client.on_publish = on_publish
client.connect(BROKER, 1883, 300)
client.loop_start()


def calculate_total_chunks(filepath):
    file_size = os.path.getsize(filepath)
    total_chunks = math.ceil(file_size / CHUNK_SIZE)
    return file_size, total_chunks


def send_metadata(filename, total_chunks, checksum, qos_level):
    global metadata_mid, metadata_sent_time

    metadata = {
        "filename": filename,
        "total_chunks": total_chunks,
        "checksum": checksum,
        "qos": qos_level,
    }
    metadata_json = json.dumps(metadata)
    metadata_payload_len = len(metadata_json.encode("utf-8"))

    metadata_sent_time = time.perf_counter()
    msg_info = client.publish(TOPIC_CTRL, metadata_json, qos=qos_level)
    metadata_mid = msg_info.mid
    return msg_info, metadata_payload_len


def send_chunks(filepath, total_chunks, qos_level):
    chunk_lengths = []
    with open(filepath, "rb") as f:
        for chunk_num in range(total_chunks):
            chunk = f.read(CHUNK_SIZE)
            chunk_lengths.append(len(chunk))

            msg_info = client.publish(TOPIC_DATA, bytearray(chunk), qos=qos_level)
            msg_info.wait_for_publish(timeout=60)

            if chunk_num % 10 == 0 or chunk_num == total_chunks - 1:
                print(f"Sent chunk {chunk_num + 1}/{total_chunks}")

    return chunk_lengths


def load_files():
    if not os.path.exists(DATA_DIR):
        print(f"Error: Directory '{DATA_DIR}' does not exist.")

    files = [
        f for f in os.listdir(DATA_DIR)
        if os.path.isfile(os.path.join(DATA_DIR, f)) and f.endswith(".bin")
    ]
    return files


def send_file(filename, qos_level):
    global ack_latency
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        print(f"File {filepath} not found.")
        return

    file_size, total_chunks = calculate_total_chunks(filepath)
    print(f"\n--- Starting transfer: {filename} ({file_size / 1024 / 1024:.2f} MB) ---")

    checksum = compute_sha256_file(filepath)

    ack_latency = 0
    metadata_ack_event.clear()

    monitor = ResourceMonitor(sample_interval=0.05)  # 50 ms granularity
    monitor.start()

    start_time = time.time()
    msg_info, metadata_payload_len = send_metadata(filename, total_chunks, checksum, qos_level)

    if not metadata_ack_event.wait(timeout=10):
        print("WARNING: Timed out waiting for metadata ACK, proceeding anyway.")

    chunk_lengths = send_chunks(filepath, total_chunks, qos_level)

    end_time = time.time()
    duration = end_time - start_time

    goodput_mbps = (file_size * 8) / (duration * 1_000_000)

    overhead_bytes = mqtt_transfer_overhead_bytes(
        TOPIC_CTRL,
        TOPIC_DATA,
        metadata_payload_len,
        chunk_lengths,
        qos_level,
        file_size,
    )
    overhead_pct = (overhead_bytes / file_size) * 100 if file_size else 0.0
    print(f"  -> Payload Overhead: {overhead_bytes:.0f} bytes ({overhead_pct:.4f}%)")

    resource_stats = monitor.stop()

    measurements = [
        {
            "protocol": "mqtt",
            "qos": qos_level,
            "side": "sender",
            "file_size": file_size / (1024 * 1024),
            "sender_duration": f"{duration:.2f}",
            "receiver_duration": "X",
            "latency": f"{ack_latency:.4f}",
            "goodput_mbps": f"{goodput_mbps:.3f}",
            "payload_overhead": f"{overhead_bytes:.0f}",
            "avg_cpu_usage": f"{resource_stats['avg_cpu_pct']:.2f}%",
            "peak_ram_usage": f"{resource_stats['peak_rss_mb']:.2f} MB",
            "energy_est": f"{resource_stats['energy_j']:.4f}"
        }
    ]
    write_to_file_mqtt(measurements)

    print("Finished sending file.")
    print(f"Latency: {ack_latency:.4f}s | Sender Time: {duration:.2f}s")

    print(f"  -> Avg CPU:    {resource_stats['avg_cpu_pct']:.2f}%")
    print(f"  -> Peak RAM:   {resource_stats['peak_rss_mb']:.2f} MB")
    print(f"  -> Energy est: {resource_stats['energy_j']:.4f} J")
    time.sleep(3)


def qos_levels_loop(files):
    for qos_level in range(0, 3):
        for filename in files:
            send_file(filename, qos_level)


if __name__ == "__main__":
    time.sleep(5)  # Wait for broker and receiver to spin up

    files = load_files()
    qos_levels_loop(files)

    time.sleep(10)
    client.loop_stop()