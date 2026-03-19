import requests
import time
import os

from output.write_csv import write_to_file

BASE_URL = "http://http-server"
DATA_DIR = "/app/data"


def transfer_binary_files():
    print(f"\n--- Phase 1: Scanning {DATA_DIR} for Binary Files ---")

    if not os.path.exists(DATA_DIR):
        print(f"Error: Directory '{DATA_DIR}' does not exist.")
        return

    # Grab all files in the directory
    files = [
        f for f in os.listdir(DATA_DIR)
        if os.path.isfile(os.path.join(DATA_DIR, f)) and f.endswith(".bin")
    ]

    if not files:
        print(f"No files found in '{DATA_DIR}' to transfer.")
        return

    print(f"Found {len(files)} file(s). Starting streaming transfers...\n")

    # Loop through and stream each file
    for filename in files:
        filepath = os.path.join(DATA_DIR, filename)
        upload_url = f"{BASE_URL}/upload/{filename}"

        # Calculate size for logging
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        print(f"Streaming {filename} ({file_size_mb:.2f} MB)...")

        try:
            # 'rb' mode combined with data=file_stream ensures chunked streaming (Low RAM usage)
            with open(filepath, "rb") as file_stream:
                start_time = time.time()

                put_response = requests.put(upload_url, data=file_stream)

                end_time = time.time()

            if put_response.status_code in [201, 204]:
                print(f"  -> Success! Transfer took {end_time - start_time:.2f} seconds.")
                measurements = [
                    {'protocol': 'http', 'filesize': file_size_mb, 'time_to_transfer': f"{end_time - start_time:.2f}"},
                ]
                write_to_file(measurements)
            else:
                print(f"  -> Failed. Status: {put_response.status_code}")
                print(f"  -> Response: {put_response.text}")

        except Exception as e:
            print(f"  -> Error transferring {filename}: {e}")


if __name__ == "__main__":
    # Wait briefly for Nginx to spin up in the Docker network
    time.sleep(5)

    # Run the batch transfer
    transfer_binary_files()

    print("\nAll transfers complete. Keeping container alive for 30s...")
    time.sleep(30)