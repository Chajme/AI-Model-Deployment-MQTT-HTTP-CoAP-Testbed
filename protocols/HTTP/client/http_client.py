import requests
import time
import os

from output.write_csv import write_to_file_http

BASE_URL = "http://http-server"
DATA_DIR = "/app/data"

def load_files():
    print(f"\n--- Phase 1: Scanning {DATA_DIR} for Binary Files ---")

    if not os.path.exists(DATA_DIR):
        print(f"Error: Directory '{DATA_DIR}' does not exist.")
        return None

    # Grab all files in the directory
    files = [
        f for f in os.listdir(DATA_DIR)
        if os.path.isfile(os.path.join(DATA_DIR, f)) and f.endswith(".bin")
    ]

    if not files:
        print(f"No files found in '{DATA_DIR}' to transfer.")
        return None

    print(f"Found {len(files)} file(s). Starting streaming transfers...\n")

    return files

def calculate_logging_size(filepath: str, filename: str):
    file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
    print(f"Streaming {filename} ({file_size_mb:.2f} MB)...")

    return file_size_mb

def measure_network_latency():
    rtt_start = time.perf_counter()
    requests.head(BASE_URL)
    network_latency = time.perf_counter() - rtt_start

    print(f"Network Latency (RTT): {network_latency:.3f}s")

    return network_latency


def calculate_payload_overhead(response):
    # 1. Request Line: "PUT /upload/file.bin HTTP/1.1\r\n"
    # We estimate this string length
    request_line = f"{response.request.method} {response.request.url} HTTP/1.1\r\n"

    # 2. Request Headers: "Host: ...\r\nContent-Length: ...\r\n"
    # Each header is "Key: Value\r\n", plus an extra \r\n at the end
    req_headers_bytes = sum(len(k) + len(v) + 4 for k, v in response.request.headers.items()) + 2

    # 3. Response Headers (The 'tax' on the return trip)
    res_headers_bytes = sum(len(k) + len(v) + 4 for k, v in response.headers.items()) + 2

    return len(request_line) + req_headers_bytes + res_headers_bytes

def transfer_binary_files():
    files = load_files()
    if not files:
        return

    # Loop through and stream each file
    for filename in files:
        time.sleep(3)

        filepath = os.path.join(DATA_DIR, filename)
        upload_url = f"{BASE_URL}/upload/{filename}"
        file_size_mb = calculate_logging_size(filepath, filename)

        latency = measure_network_latency()

        try:
            # 'rb' mode combined with data=file_stream ensures chunked streaming (Low RAM usage)
            with open(filepath, "rb") as file_stream:
                start_time = time.time()
                put_response = requests.put(upload_url, data=file_stream)
                end_time = time.time()

            if put_response.status_code in [201, 204]:
                transfer_time = end_time - start_time

                header_overhead = calculate_payload_overhead(put_response)

                # Estimate Chunking Overhead (Requests typically uses 8KB chunks for files)
                # Formula: (Total Bytes / 8192) * ~10 bytes for hex size + CRLFs
                file_size_bytes = os.path.getsize(filepath)
                chunk_overhead = (file_size_bytes / 8192) * 10

                total_overhead_bytes = header_overhead + chunk_overhead
                overhead_percentage = (total_overhead_bytes / file_size_bytes) * 100

                print(f"  -> Success! Transfer took {transfer_time:.2f} seconds.")
                print(f"  -> Latency was {latency:.2f} seconds.")
                print(f"  -> Payload Overhead: {total_overhead_bytes:.0f} bytes ({overhead_percentage:.4f}%)")

                measurements = [
                    {'protocol': 'http',
                     'file_size': file_size_mb,
                     'time_to_transfer': f"{transfer_time:.3f}",
                     'latency': f"{latency:.5f}",
                     'payload_overhead': f"{total_overhead_bytes:.0F}"
                     }
                ]
                write_to_file_http(measurements)

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