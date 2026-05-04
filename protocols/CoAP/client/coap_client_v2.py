import asyncio
import math
import time
import os
from urllib.parse import urlparse
# import logging

from aiocoap import Message, Context, PUT, GET
from aiocoap.numbers.constants import MAX_REGULAR_BLOCK_SIZE_EXP

from output.integrity_checker import compute_sha256_file, sha256
from output.resource_monitor import ResourceMonitor
from output.write_csv import write_to_file_coap, write_to_file_coap_2

# logging.basicConfig(level=logging.INFO)
# logging.getLogger("aiocoap").setLevel(logging.DEBUG)
#
# class RetransmitCounter(logging.Handler):
#     def __init__(self):
#         super().__init__()
#         self.count = 0
#
#     def emit(self, record):
#         msg = record.getMessage().lower()
#
#         # catch both message retransmits + blockwise retries
#         if "retransmit" in msg or "timeout" in msg:
#             self.count += 1

# Use the largest standard CoAP block: SZX=6 -> 2^(4+6) = 1024 bytes
_BLOCK_SZX = MAX_REGULAR_BLOCK_SIZE_EXP  # 6
_BLOCK_SIZE = 2 ** (4 + _BLOCK_SZX)      # 1024 bytes

# Per-block wire overhead constants (bytes), MQTT 3.1.1 / CoAP RFC 7252:
#   CoAP fixed header:  4 bytes
#   Token:              8 bytes (conservative upper bound)
#   Block1 option:      3 bytes (option delta + length + value)
#   ACK from server:    4 bytes per CON block (fixed header only, no token/options)
_COAP_HEADER   = 4
_TOKEN         = 8
_BLOCK1_OPTION = 3
_ACK_HEADER    = 4  # server → client, one per CON block

DATA_DIR   = "/app/data"
SERVER_URI = "coap://coap-server/upload"
MAX_RETRIES = 3


async def get_latency(context: Context, uri: str) -> float:
    """
    Measure CoAP round-trip latency using GET /.well-known/core — a standard
    discovery endpoint every CoAP server is expected to serve. This avoids
    sending a GET to a PUT-only resource (which would return 4.05 Method Not
    Allowed and measure an error round-trip instead of real latency).
    """
    parsed = urlparse(uri)
    ping_uri = f"coap://{parsed.hostname}/.well-known/core"
    try:
        req = Message(code=GET, uri=ping_uri)
        t0 = time.perf_counter()
        await context.request(req).response
        return time.perf_counter() - t0
    except Exception:
        return 0.0


def calculate_payload_overhead(filename: str, checksum: str, file_size_bytes: int) -> int:
    """
    Estimate total CoAP wire overhead for a blockwise PUT transfer.

    For each 1024-byte block the client sends a CON PUBLISH with:
      - 4-byte fixed header
      - 8-byte token
      - 3-byte Block1 option
    The server ACKs each CON block with a 4-byte fixed header.

    The Uri-Path and Uri-Query options are sent once on the first block:
      - "upload" path segment: ~8 bytes
      - file=<filename>:       len(filename) + ~4 bytes option header
      - checksum=<hex>:        len(checksum) + ~4 bytes option header
    """
    num_blocks = math.ceil(file_size_bytes / _BLOCK_SIZE)

    # Per-block overhead (client → server + server → client ACK)
    per_block = (_COAP_HEADER + _TOKEN + _BLOCK1_OPTION) + _ACK_HEADER

    # One-time Uri-Path + Uri-Query options on the first block
    uri_path_overhead  = 8                          # "upload" path segment
    uri_query_overhead = (len(filename) + 4) + (len(checksum) + 4)

    return per_block * num_blocks + uri_path_overhead + uri_query_overhead


def load_files() -> list[str]:
    return [
        f for f in os.listdir(DATA_DIR)
        if os.path.isfile(os.path.join(DATA_DIR, f)) and f.endswith(".bin")
    ]


async def transfer_file(context: Context, filename: str) -> None:
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        print(f"File {filename} not found.")
        return

    # counter = RetransmitCounter()
    # logging.getLogger("aiocoap").addHandler(counter)
    # logging.getLogger("aiocoap").setLevel(logging.DEBUG)

    # NOTE: aiocoap requires the full payload in memory to perform blockwise
    # transfers automatically. Unlike the HTTP client there is no streaming
    # path here — for very large files this will be the primary RAM cost.
    with open(filepath, "rb") as f:
        payload = f.read()

    file_size_bytes = len(payload)
    file_size_mb    = file_size_bytes / (1024 * 1024)
    print(f"\n--- CoAP Transfer: {filename} ({file_size_mb:.2f} MB) ---")

    checksum = sha256(payload)
    latency  = await get_latency(context, SERVER_URI)

    retries       = 0
    response      = None
    goodput_mbps  = 0.0
    transfer_time = 0.0

    monitor = ResourceMonitor(sample_interval=0.01)  # 50 ms granularity
    monitor.start()

    for attempt in range(MAX_RETRIES):
        try:
            request = Message(
                code=PUT,
                payload=payload,
                uri=f"{SERVER_URI}?file={filename}&checksum={checksum}",
            )
            t0       = time.perf_counter()
            response = await context.request(request).response
            transfer_time = max(time.perf_counter() - t0, 0.001)
            goodput_mbps  = (file_size_bytes * 8) / (transfer_time * 1_000_000)
            break
        except Exception as e:
            retries += 1
            print(f"  Attempt {attempt + 1} failed: {e}. Retrying...")
            await asyncio.sleep(2 ** attempt)  # exponential back-off

    if response is None:
        print(f"  Transfer failed after {MAX_RETRIES} attempts.")
        return

    # integrity_ok relies on the server verifying the checksum query parameter
    # and returning 4.00 Bad Request on mismatch; 2.04 Changed means the server
    # confirmed the file was saved with a matching checksum.
    integrity_ok = response.code.is_successful()
    resource_stats = monitor.stop()
    total_overhead = calculate_payload_overhead(filename, checksum, file_size_bytes)
    overhead_pct   = (total_overhead / file_size_bytes) * 100

    print(f"Result: {response.code} | Time: {transfer_time:.2f}s | Retries: {retries}")
    print(f"Latency: {latency:.4f}s | Overhead: {total_overhead} bytes ({overhead_pct:.4f}%)")
    # block_retries = counter.count
    # print("Block-level retransmissions:", block_retries)
    print(f"  -> Avg CPU:    {resource_stats['avg_cpu_pct']:.2f}%")
    print(f"  -> Peak RAM:   {resource_stats['peak_rss_mb']:.2f} MB")
    print(f"  -> Energy est: {resource_stats['energy_j']:.4f} J")

    write_to_file_coap_2([{
        "protocol":        "coap",
        "file_size":       file_size_mb,
        "time_to_transfer": f"{transfer_time:.2f}",
        "latency":         f"{latency:.4f}",
        "payload_overhead": f"{total_overhead:.0f}",
        "overhead_pct":    f"{overhead_pct:.4f}",
        "goodput_mbps":    f"{goodput_mbps:.3f}",
        "integrity_ok":    integrity_ok,
        "avg_cpu_usage": f"{resource_stats['avg_cpu_pct']:.2f}%",
        "peak_ram_usage": f"{resource_stats['peak_rss_mb']:.2f} MB",
        "energy_est": f"{resource_stats['energy_j']:.4f}"
    }])

    time.sleep(3)


async def transfer_all_files() -> None:
    files = load_files()
    if not files:
        print("No .bin files found.")
        return
    context = await Context.create_client_context()
    for filename in sorted(files):
        await transfer_file(context, filename)


async def main() -> None:
    await asyncio.sleep(5)  # Wait for the server container to spin up
    await transfer_all_files()
    print("\nTransfer complete. Keeping alive for 30 seconds.")
    await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(main())