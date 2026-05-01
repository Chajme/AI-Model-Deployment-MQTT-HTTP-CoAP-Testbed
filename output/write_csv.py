import csv
import os

def _measurement_file(protocol: str) -> str:
    # Optional suffix keeps baseline and chaos runs in separate CSV files.
    suffix = os.getenv("MEASUREMENT_SUFFIX", "").strip()
    safe_suffix = suffix.replace(" ", "_")
    return f"/app/output/{protocol}_measurements{safe_suffix}.csv"

def write_to_csv(output_path: str, fieldnames: list, data: list[dict]):
    if not data:
        return

    file_exists = os.path.isfile(output_path)

    with open(output_path, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        writer.writerows(data)
        print(f"Writing to {output_path}")

def write_to_file_http(data: list[dict]):
    output_path = _measurement_file("http")
    fieldnames = [
        'protocol',
        'file_size',
        'time_to_transfer',
        'latency',
        'payload_overhead',
        'goodput_mbps',
        'integrity_ok'
    ]
    write_to_csv(output_path, fieldnames, data)

def write_to_file_mqtt(data: list[dict]):
    output_path = _measurement_file("mqtt")
    fieldnames = [
        'protocol',
        'qos',
        'side',
        'file_size',
        'sender_duration',
        'receiver_duration',
        'latency',
        'payload_overhead',
        'goodput_mbps',
        'integrity_ok'
    ]
    write_to_csv(output_path, fieldnames, data)

def write_to_file_coap(data: list[dict]):
    output_path = _measurement_file("coap")
    fieldnames = [
        'protocol',
        'file_size',
        'time_to_transfer',
        'latency',
        'payload_overhead',
        'goodput_mbps',
        'integrity_ok'
    ]
    write_to_csv(output_path, fieldnames, data)
