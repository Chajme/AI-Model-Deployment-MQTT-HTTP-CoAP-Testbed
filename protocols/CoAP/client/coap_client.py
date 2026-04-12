import asyncio
import math
import time
import os
from aiocoap import Message, Context, PUT, GET
from aiocoap.numbers.constants import MAX_REGULAR_BLOCK_SIZE_EXP

from output.write_csv import write_to_file_coap

# Block-wise transfer uses 2^(4+szx) byte blocks; default szx matches MAX_REGULAR_BLOCK_SIZE_EXP (1024 B).
_BLOCK_SIZE = 2 ** (4 + MAX_REGULAR_BLOCK_SIZE_EXP)
# Approximate extra bytes per additional PUT datagram (header + token + Block1 + payload marker).
_EXTRA_FRAMING_PER_BLOCK = 24

DATA_DIR = "/app/data"
SERVER_URI = "coap://coap-server/upload"


async def get_latency(context, uri):
    ping_request = Message(code=GET, uri=uri)

    start_ping = time.perf_counter()
    try:
        # We don't care if the response is 4.05 Method Not Allowed,
        # we just need the server to acknowledge us.
        await context.request(ping_request).response
        end_ping = time.perf_counter()
        return end_ping - start_ping
    except Exception:
        return 0.0

def calculate_file_size(payload, filename: str):
    file_size_mb = len(payload) / (1024 * 1024)
    print(f"\n--- Starting CoAP Transfer: {filename} ({file_size_mb:.2f} MB) ---")
    return file_size_mb

def load_files():
    files = [
        f for f in os.listdir(DATA_DIR)
        if os.path.isfile(os.path.join(DATA_DIR, f)) and f.endswith(".bin")
    ]

    return files


def calculate_payload_overhead(request_msg: Message, response_msg: Message, file_size_bytes: int) -> float:
    """Non-payload CoAP bytes: logical encode (URI/options) plus repeated framing for block-wise PUTs."""
    try:
        req_oh = len(request_msg.encode()) - file_size_bytes
        res_payload_len = len(response_msg.payload or b"")
        res_oh = len(response_msg.encode()) - res_payload_len
    except Exception:
        req_oh = 48.0
        res_oh = 32.0
    num_blocks = max(1, math.ceil(file_size_bytes / _BLOCK_SIZE))
    block_extra = (num_blocks - 1) * _EXTRA_FRAMING_PER_BLOCK
    return req_oh + res_oh + block_extra


async def transfer_file(context, filename):
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        print(f"File {filename} not found.")
        return

    # Load file into memory
    with open(filepath, "rb") as f:
        payload = f.read()

    file_size_mb = calculate_file_size(payload, filename)

    latency = await get_latency(context, SERVER_URI)

    start_time = time.time()

    # Pass the filename as a URI query: ?file=filename.bin
    request = Message(code=PUT, payload=payload, uri=f"{SERVER_URI}?file={filename}")

    try:
        response = await context.request(request).response
        end_time = time.time()

        # Prevent division by zero
        transfer_time = max(end_time - start_time, 0.001)

        print(f"Result: {response.code}")
        print(f"Latency (Time to Start): {latency:.4f}s")
        print(f"CoAP Time: {transfer_time:.2f}s ({file_size_mb / transfer_time:.2f} MB/s)")

        file_size_bytes = len(payload)
        total_overhead_bytes = calculate_payload_overhead(request, response, file_size_bytes)
        overhead_pct = (total_overhead_bytes / file_size_bytes) * 100 if file_size_bytes else 0.0
        print(f"  -> Payload Overhead: {total_overhead_bytes:.0f} bytes ({overhead_pct:.4f}%)")

        measurements = [
            {'protocol': "coap",
             'file_size': file_size_mb,
             'time_to_transfer': f"{transfer_time:.2f}",
             'latency': f"{latency:.4f}",
             'payload_overhead': f"{total_overhead_bytes:.0F}",
            }
        ]

        write_to_file_coap(measurements)

    except Exception as e:
        print(f"Failed to send {filename}: {e}")

async def transfer_all_files():
    files = load_files()
    if not files:
        print("No .bin files found.")
        return

    # Create the client context once and reuse it
    context = await Context.create_client_context()
    for filename in files:
        await transfer_file(context, filename)


async def main():
    # Wait for the server container to spin up
    await asyncio.sleep(5)

    await transfer_all_files()
    print("\nTransfer complete. Keeping alive for 30 seconds.")

    # Keep alive so Docker doesn't exit immediately
    await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())