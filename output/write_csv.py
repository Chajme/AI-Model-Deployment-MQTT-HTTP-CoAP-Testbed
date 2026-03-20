import csv
import os


def write_to_file_http(data: list[dict]):
    output_path = "/app/output/http_measurements.csv"
    file_exists = os.path.isfile(output_path)

    with open(output_path, 'a', newline='') as csvfile:
        fieldnames = ['protocol', 'filesize', 'time_to_transfer']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        writer.writerows(data)
        print(f"Writing to {output_path}")

def write_to_file_mqtt(data: list[dict]):
    output_path = "/app/output/mqtt_measurements.csv"
    file_exists = os.path.isfile(output_path)

    with open(output_path, 'a', newline='') as csvfile:
        fieldnames = ['protocol', 'qos', 'side', 'file_size', 'sender_duration', 'receiver_duration']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        writer.writerows(data)
        print(f"Writing to {output_path}")
