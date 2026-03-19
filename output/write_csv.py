import csv
import os


def write_to_file(data: list[dict]):
    output_path = "/app/output/protocols_measurements.csv"
    file_exists = os.path.isfile(output_path)

    with open(output_path, 'a', newline='') as csvfile:
        fieldnames = ['protocol', 'filesize', 'time_to_transfer']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        writer.writerows(data)
        print(f"Writing to {output_path}")
