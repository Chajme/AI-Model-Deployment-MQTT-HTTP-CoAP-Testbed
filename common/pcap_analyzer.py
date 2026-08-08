import csv
import subprocess
from pathlib import Path


def run_tshark(pcap_file, display_filter=None, fields=None):
    """
    Run tshark and return rows of extracted fields.
    """
    command = [
        "tshark",
        "-r",
        str(pcap_file),
        "-T",
        "fields",
        "-E",
        "separator=,",
        "-E",
        "quote=d",
        "-E",
        "header=y",
    ]

    if display_filter:
        command.extend(["-Y", display_filter])

    for field in fields:
        command.extend(["-e", field])

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=True,
    )

    return result.stdout


def analyze_pcap(pcap_file, file_size_bytes, qos_level, filename):
    """
    Analyze one MQTT PCAP.

    file_size_bytes:
        Actual size of the binary file being transferred.

    Returns:
        Dictionary containing the measurements.
    """

    pcap_file = Path(pcap_file)

    if not pcap_file.exists():
        raise FileNotFoundError(f"PCAP does not exist: {pcap_file}")

    # ---------------------------------------------------------
    # 1. All captured frames
    # ---------------------------------------------------------

    output = run_tshark(
        pcap_file,
        fields=[
            "frame.number",
            "frame.len",
            "frame.time_relative",
            "ip.proto",
        ],
    )

    lines = output.strip().splitlines()

    # Remove header
    if len(lines) <= 1:
        raise RuntimeError("PCAP contains no packets.")

    packet_rows = lines[1:]

    total_packets = 0
    total_wire_bytes = 0
    last_timestamp = 0.0

    for line in packet_rows:
        if not line.strip():
            continue

        parts = list(csv.reader([line]))[0]

        if len(parts) < 3:
            continue

        try:
            frame_len = int(parts[1])
            timestamp = float(parts[2])
        except ValueError:
            continue

        total_packets += 1
        total_wire_bytes += frame_len
        last_timestamp = max(last_timestamp, timestamp)

    duration = last_timestamp

    # ---------------------------------------------------------
    # 2. MQTT packets
    # ---------------------------------------------------------

    mqtt_output = run_tshark(
        pcap_file,
        display_filter="mqtt",
        fields=[
            "frame.number",
            "frame.len",
            "mqtt.msgtype",
        ],
    )

    mqtt_lines = mqtt_output.strip().splitlines()

    mqtt_packets = 0
    mqtt_wire_bytes = 0
    mqtt_packet_types = {}

    if len(mqtt_lines) > 1:
        for line in mqtt_lines[1:]:
            if not line.strip():
                continue

            parts = list(csv.reader([line]))[0]

            if len(parts) < 3:
                continue

            try:
                frame_len = int(parts[1])
            except ValueError:
                continue

            msg_type = parts[2] if parts[2] else "UNKNOWN"

            mqtt_packets += 1
            mqtt_wire_bytes += frame_len

            mqtt_packet_types[msg_type] = (
                mqtt_packet_types.get(msg_type, 0) + 1
            )

    # ---------------------------------------------------------
    # 3. TCP retransmissions
    # ---------------------------------------------------------

    retransmission_output = run_tshark(
        pcap_file,
        display_filter=(
            "tcp.analysis.retransmission or "
            "tcp.analysis.fast_retransmission or "
            "tcp.analysis.spurious_retransmission"
        ),
        fields=[
            "frame.number",
        ],
    )

    retransmission_lines = [
        line
        for line in retransmission_output.strip().splitlines()[1:]
        if line.strip()
    ]

    tcp_retransmissions = len(retransmission_lines)

    # ---------------------------------------------------------
    # 4. Calculate derived metrics
    # ---------------------------------------------------------

    total_overhead_bytes = total_wire_bytes - file_size_bytes

    if file_size_bytes > 0:
        overhead_percentage = (
            total_overhead_bytes / file_size_bytes
        ) * 100
    else:
        overhead_percentage = 0

    if duration > 0:
        wire_throughput_mbps = (
            total_wire_bytes * 8
        ) / (duration * 1_000_000)

        goodput_mbps = (
            file_size_bytes * 8
        ) / (duration * 1_000_000)
    else:
        wire_throughput_mbps = 0
        goodput_mbps = 0

    return {
        "filename": filename,
        "qos": qos_level,
        "file_size_bytes": file_size_bytes,
        "total_packets": total_packets,
        "total_wire_bytes": total_wire_bytes,
        "mqtt_packets": mqtt_packets,
        "mqtt_wire_bytes": mqtt_wire_bytes,
        "tcp_retransmissions": tcp_retransmissions,
        "duration_seconds": duration,
        "total_overhead_bytes": total_overhead_bytes,
        "overhead_percentage": overhead_percentage,
        "wire_throughput_mbps": wire_throughput_mbps,
        "goodput_mbps": goodput_mbps,
        "mqtt_packet_types": mqtt_packet_types,
    }


def print_result(result):
    print("\n========== PCAP ANALYSIS ==========")

    print(f"File:                  {result['filename']}")
    print(f"QoS:                   {result['qos']}")

    print("\n--- Traffic ---")

    print(f"File size:             {result['file_size_bytes']:,} B")
    print(f"Captured packets:      {result['total_packets']:,}")
    print(f"Captured bytes:        {result['total_wire_bytes']:,} B")
    print(f"MQTT packets:          {result['mqtt_packets']:,}")
    print(f"MQTT frame bytes:      {result['mqtt_wire_bytes']:,} B")

    print("\n--- Overhead ---")

    print(
        f"Total overhead:        "
        f"{result['total_overhead_bytes']:,} B"
    )

    print(
        f"Overhead percentage:   "
        f"{result['overhead_percentage']:.2f}%"
    )

    print("\n--- Performance ---")

    print(
        f"Duration:              "
        f"{result['duration_seconds']:.4f} s"
    )

    print(
        f"Goodput:               "
        f"{result['goodput_mbps']:.3f} Mbps"
    )

    print(
        f"Wire throughput:       "
        f"{result['wire_throughput_mbps']:.3f} Mbps"
    )

    print(
        f"TCP retransmissions:   "
        f"{result['tcp_retransmissions']}"
    )

    print("\n--- MQTT packet types ---")

    for packet_type, count in result["mqtt_packet_types"].items():
        print(f"{packet_type:20} {count}")

    print("===================================\n")


if __name__ == "__main__":
    # import argparse
    #
    # parser = argparse.ArgumentParser(
    #     description="Analyze an MQTT PCAP using TShark."
    # )
    #
    # parser.add_argument(
    #     "pcap",
    #     help="Path to the PCAP file"
    # )
    #
    # parser.add_argument(
    #     "--file-size",
    #     type=int,
    #     required=True,
    #     help="Original file size in bytes"
    # )
    #
    # parser.add_argument(
    #     "--filename",
    #     required=True,
    #     help="Original filename"
    # )
    #
    # parser.add_argument(
    #     "--qos",
    #     type=int,
    #     required=True,
    #     choices=[0, 1, 2],
    #     help="MQTT QoS level"
    # )
    #
    # args = parser.parse_args()

    pcap = "output/pcap/mqtt_binary_file_1mb.bin_1.pcap"
    file_size = 1024 * 1024
    filename = "binary_file_1mb.bin"
    qos_level = 1


    result = analyze_pcap(
        pcap,
        file_size,
        qos_level,
        filename,
    )

    print_result(result)