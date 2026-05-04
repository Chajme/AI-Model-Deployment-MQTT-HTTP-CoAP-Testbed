import asyncio
import math
import time
import os
from aiocoap import Message, Context, PUT, GET
from aiocoap.numbers.constants import MAX_REGULAR_BLOCK_SIZE_EXP

from output.integrity_checker import compute_sha256_file, sha256
from output.resource_monitor import ResourceMonitor
from output.write_csv import write_to_file_coap

# Use the largest standard CoAP block: SZX=6 -> 2^(4+6) = 1024 bytes
# For non-constrained servers you may negotiate SZX=7 (non-standard, 2048 B)
_BLOCK_SZX = MAX_REGULAR_BLOCK_SIZE_EXP  # 6
_BLOCK_SIZE = 2 ** (4 + _BLOCK_SZX)      # 1024 bytes
_EXTRA_FRAMING_PER_BLOCK = 24


DATA_DIR = "/app/data"
SERVER_URI = "coap://coap-server/upload"
MAX_RETRIES = 3

async def get_latency(context, uri):
    try:
        req = Message(code=GET, uri=uri)
        t0 = time.perf_counter()
        await context.request(req).response
        return time.perf_counter() - t0
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


def calculate_payload_overhead(request_msg, response_msg, file_size_bytes):
    try:
        req_oh = len(request_msg.encode()) - file_size_bytes
        res_oh = len(response_msg.encode()) - len(response_msg.payload or b"")
    except Exception:
        req_oh, res_oh = 48, 32
    num_blocks = max(1, math.ceil(file_size_bytes / _BLOCK_SIZE))
    return req_oh + res_oh + (num_blocks - 1) * _EXTRA_FRAMING_PER_BLOCK



async def transfer_file(context, filename):
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        print(f"File {filename} not found.")
        return

    with open(filepath, "rb") as f:
        payload = f.read()

        checksum = sha256(payload)

    file_size_bytes = len(payload)
    file_size_mb = file_size_bytes / (1024 * 1024)
    print(f"\n--- CoAP Transfer: {filename} ({file_size_mb:.2f} MB) ---")

    latency = await get_latency(context, SERVER_URI)
    retries = 0
    response = None
    goodput_mbps = 0.0
    transfer_time = 0.0





    for attempt in range(MAX_RETRIES):
        try:
            request = Message(
                code=PUT,
                payload=payload,
                uri=f"{SERVER_URI}?file={filename}&checksum={checksum}",
            )
            t0 = time.time()
            response = await context.request(request).response
            transfer_time = max(time.time() - t0, 0.001)
            goodput_mbps = (file_size_bytes * 8) / (transfer_time * 1_000_000)
            break
        except Exception as e:
            retries += 1
            print(f"  Attempt {attempt+1} failed: {e}. Retrying...")
            await asyncio.sleep(2 ** attempt)  # exponential back-off

    if response is None:
        print(f"  Transfer failed after {MAX_RETRIES} attempts.")
        return

    integrity_ok = response.code.is_successful()



    total_overhead = calculate_payload_overhead(request, response, file_size_bytes)
    overhead_pct = (total_overhead / file_size_bytes) * 100
    print(f"Result: {response.code} | Time: {transfer_time:.2f}s | Retries: {retries}")

    write_to_file_coap([{
        "protocol": "coap",
        "file_size": file_size_mb,
        "time_to_transfer": f"{transfer_time:.2f}",
        "latency": f"{latency:.4f}",
        "payload_overhead": f"{total_overhead:.0f}",
        "goodput_mbps": f"{goodput_mbps:.3f}",
        "integrity_ok": integrity_ok
    }])




async def transfer_all_files():
    files = [f for f in os.listdir(DATA_DIR)
             if os.path.isfile(os.path.join(DATA_DIR, f)) and f.endswith(".bin")]
    if not files:
        print("No .bin files found.")
        return
    context = await Context.create_client_context()
    for filename in sorted(files):
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