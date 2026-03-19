import requests
import time
import os

from output.write_csv import write_to_file

BASE_URL = "http://http-server"
DATA_DIR = "/app/data"

# def test_communication():
#     print("--- Phase 1: Connection Check ---")
#     try:
#         response = requests.get(f"{BASE_URL}/")
#         if response.status_code == 200:
#             print(f"Success! Reached Nginx server. Status: {response.status_code}")
#         else:
#             print(f"Server reached, but got status: {response.status_code}")
#     except Exception as e:
#         print(f"Connection failed: {e}")
#         return
#
#     print("\n--- Phase 2: Basic Data Transfer ---")
#     test_message = "Protocol Test: Hello Nginx!"
#     upload_url = f"{BASE_URL}/upload/hello.txt"
#
#     try:
#         print(f"Sending text to {upload_url}...")
#         put_response = requests.put(upload_url, data=test_message)
#
#         if put_response.status_code in [201, 204]:
#             print("Success! Data received and saved by Nginx.")
#         else:
#             print(f"Transfer failed. Status: {put_response.status_code}")
#             print(f"Response Body: {put_response.text}")
#
#     except Exception as e:
#         print(f"Data transfer failed: {e}")
#
#
# if __name__ == "__main__":
#     time.sleep(5)
#     test_communication()
#     print("Test complete. Keeping container alive for 30s...")
#     time.sleep(30)

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