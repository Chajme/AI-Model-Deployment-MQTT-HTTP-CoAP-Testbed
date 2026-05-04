import requests
import time
import os
import socket
from urllib.parse import urlparse
import math
import struct

from output.integrity_checker import compute_sha256_file
from output.resource_monitor import ResourceMonitor
from output.write_csv import write_to_file_http_2

BASE_URL = "http://http-server:8000"
DATA_DIR = "/app/data"

TCP_HEADER = 20
IP_HEADER = 20
TCP_OVERHEAD_PER_PACKET = TCP_HEADER + IP_HEADER  # 40 bytes

def get_tcp_mss():
    parsed = urlparse("http://http-server:8000")
    host = parsed.hostname
    port = parsed.port or 80

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))

    TCP_INFO = 11  # from linux tcp.h

    info = s.getsockopt(socket.IPPROTO_TCP, TCP_INFO, 104)

    # tcpi_snd_mss is at offset 16 (Linux-specific struct layout)
    snd_mss = struct.unpack_from("I", info, 16)[0]

    s.close()
    return snd_mss

def estimate_tcp_segments(file_size_bytes: int) -> int:
    try:
        MSS = get_tcp_mss()
    except Exception:
        MSS = 1460
    return math.ceil(file_size_bytes / MSS)

def estimate_http_transport_overhead(file_size_bytes: int) -> int:
    segments = estimate_tcp_segments(file_size_bytes)

    # Data packets
    data_overhead = segments * TCP_OVERHEAD_PER_PACKET

    # ACK packets (assume delayed ACK: 1 per 2 segments)
    ack_packets = segments // 2
    ack_overhead = ack_packets * TCP_OVERHEAD_PER_PACKET

    # TCP handshake (SYN, SYN-ACK, ACK)
    handshake = 3 * TCP_OVERHEAD_PER_PACKET

    return data_overhead + ack_overhead + handshake

def calculate_total_http_overhead(response, file_size_bytes: int) -> int:
    http_overhead = calculate_payload_overhead(response)
    transport_overhead = estimate_http_transport_overhead(file_size_bytes)

    return http_overhead + transport_overhead



def load_files():
    print(f"\n--- Phase 1: Scanning {DATA_DIR} for Binary Files ---")

    if not os.path.exists(DATA_DIR):
        print(f"Error: Directory '{DATA_DIR}' does not exist.")
        return None

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


def measure_network_latency() -> float:
    """
    Measure raw TCP connection latency to the server by timing a bare
    socket connect/disconnect — no HTTP overhead, no server processing time.
    This is a clean RTT measurement comparable to a TCP ping.
    """
    parsed = urlparse(BASE_URL)
    host = parsed.hostname
    port = parsed.port or 80

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        start = time.perf_counter()
        sock.connect((host, port))
        latency = time.perf_counter() - start
    finally:
        sock.close()

    print(f"Network Latency (TCP RTT): {latency:.5f}s")
    return latency


def calculate_payload_overhead(response: requests.Response) -> int:
    """
    HTTP/1.1 wire overhead = request line + request headers + response headers.

    Chunked transfer encoding is NOT added here because requests sends the file
    with a known Content-Length (continuous body), so Transfer-Encoding: chunked
    is not used and there is no per-chunk framing on the wire.
    """
    # 1. Request line: "PUT /upload/file.bin HTTP/1.1\r\n"
    parsed = urlparse(response.request.url)
    path = parsed.path + (f"?{parsed.query}" if parsed.query else "")
    request_line = f"{response.request.method} {path} HTTP/1.1\r\n"

    # 2. Request headers: each "Key: Value\r\n" + blank line "\r\n"
    req_headers_bytes = (
        sum(len(k) + len(v) + 4 for k, v in response.request.headers.items()) + 2
    )

    # 3. Response status line: "HTTP/1.1 200 OK\r\n"
    reason = response.reason or ""
    response_line = f"HTTP/1.1 {response.status_code} {reason}\r\n"

    # 4. Response headers: each "Key: Value\r\n" + blank line "\r\n"
    res_headers_bytes = (
        sum(len(k) + len(v) + 4 for k, v in response.headers.items()) + 2
    )

    return len(request_line) + req_headers_bytes + len(response_line) + res_headers_bytes


def transfer_binary_files():
    files = load_files()
    if not files:
        return

    for filename in files:
        filepath = os.path.join(DATA_DIR, filename)
        checksum = compute_sha256_file(filepath)
        upload_url = f"{BASE_URL}/upload/{filename}"
        file_size_mb = calculate_logging_size(filepath, filename)
        file_size_bytes = os.path.getsize(filepath)

        latency = measure_network_latency()

        monitor = ResourceMonitor(sample_interval=0.01)  # 50 ms granularity
        monitor.start()

        try:
            ttfb = None

            def record_ttfb(response, **kwargs):
                """
                requests response hook: fires as soon as the response headers
                arrive, before the body is read. Used to capture TTFB.
                """
                nonlocal ttfb
                ttfb = time.perf_counter() - request_start

            with open(filepath, "rb") as file_stream:
                request_start = time.perf_counter()
                put_response = requests.put(
                    upload_url,
                    data=file_stream,
                    headers={"X-Checksum": checksum},
                    hooks={"response": record_ttfb},
                )
            end_time = time.perf_counter()

            transfer_time = end_time - request_start

            # FIX #3: integrity is determined by the server verifying the
            # X-Checksum header and returning 200 only on a match.
            # Document the assumption: a 200 response means the server
            # confirmed checksum equality server-side.
            integrity_ok = (put_response.status_code == 200)

            # FIX #2: removed phantom chunk_overhead — requests sends the file
            # as a continuous body with Content-Length, not chunked encoding.
            # header_overhead = calculate_payload_overhead(put_response)
            # total_overhead_bytes = header_overhead
            total_overhead_bytes = calculate_total_http_overhead(
                put_response,
                file_size_bytes
            )

            overhead_percentage = (total_overhead_bytes / file_size_bytes) * 100
            goodput_mbps = (file_size_bytes * 8) / (transfer_time * 1_000_000)

            if integrity_ok:
                print(f"  -> Success! Transfer took {transfer_time:.2f}s.")
                print(f"  -> TTFB: {ttfb:.5f}s")
                print(f"  -> TCP RTT Latency: {latency:.5f}s")
                print(f"  -> Overhead: {total_overhead_bytes} bytes ({overhead_percentage:.4f}%)")
                print(f"  -> Integrity OK: {integrity_ok}")
            else:
                print(f"  -> Failed. Status: {put_response.status_code}")
                print(f"  -> Integrity OK: {integrity_ok}")

            resource_stats = monitor.stop()

            print(f"  -> Avg CPU:    {resource_stats['avg_cpu_pct']:.2f}%")
            print(f"  -> Peak RAM:   {resource_stats['peak_rss_mb']:.2f} MB")
            print(f"  -> Energy est: {resource_stats['energy_j']:.4f} J")

            measurements = [
                {
                    "protocol": "http",
                    "file_size": file_size_mb,
                    "time_to_transfer": f"{transfer_time:.3f}",
                    # FIX #1: log both TCP RTT and TTFB as distinct metrics.
                    "latency_tcp_rtt": f"{latency:.5f}",
                    "latency_ttfb": f"{ttfb:.5f}" if ttfb is not None else "X",
                    # FIX #4: lowercase 'f' format specifier.
                    "payload_overhead": f"{total_overhead_bytes:.0f}",
                    # FIX #5: overhead_percentage now logged.
                    "overhead_pct": f"{overhead_percentage:.4f}",
                    "goodput_mbps": f"{goodput_mbps:.3f}",
                    "integrity_ok": integrity_ok,
                    "avg_cpu_usage": f"{resource_stats['avg_cpu_pct']:.2f}%",
                    "peak_ram_usage": f"{resource_stats['peak_rss_mb']:.2f} MB",
                    "energy_est": f"{resource_stats['energy_j']:.4f}"
                }
            ]
            write_to_file_http_2(measurements)

        except Exception as e:
            print(f"  -> Error transferring {filename}: {e}")


if __name__ == "__main__":
    # Wait briefly for the HTTP server to spin up in the Docker network
    time.sleep(5)

    transfer_binary_files()

    print("\nAll transfers complete. Keeping container alive for 30s...")
    time.sleep(30)