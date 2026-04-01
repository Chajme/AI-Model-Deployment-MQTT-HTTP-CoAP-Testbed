import asyncio
import time
import os
from aiocoap import Message, Context, PUT

from output.write_csv import write_to_file_coap

DATA_DIR = "/app/data"
SERVER_URI = "coap://coap-server/upload"

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

async def transfer_file(context, filename):
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        print(f"File {filename} not found.")
        return

    # Load file into memory
    with open(filepath, "rb") as f:
        payload = f.read()

    file_size_mb = calculate_file_size(payload, filename)

    start_time = time.time()

    # Pass the filename as a URI query: ?file=filename.bin
    request = Message(code=PUT, payload=payload, uri=f"{SERVER_URI}?file={filename}")

    try:
        response = await context.request(request).response
        end_time = time.time()

        # Prevent division by zero
        transfer_time = max(end_time - start_time, 0.001)

        print(f"Result: {response.code}")
        print(f"CoAP Time: {transfer_time:.2f}s ({file_size_mb / transfer_time:.2f} MB/s)")

        measurements = [
            {'protocol': "coap",
             'file_size': file_size_mb,
             'time_to_transfer': f"{transfer_time:.2f}"}
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