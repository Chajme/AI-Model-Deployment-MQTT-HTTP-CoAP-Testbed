import asyncio
import time
import os
from aiocoap import Message, Context, PUT

DATA_DIR = "/app/data"
SERVER_URI = "coap://coap-server/upload"


async def transfer_file(context, filename):
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        print(f"File {filename} not found.")
        return

    # Load file into memory
    with open(filepath, "rb") as f:
        payload = f.read()

    file_size_mb = len(payload) / (1024 * 1024)
    print(f"\n--- Starting CoAP Transfer: {filename} ({file_size_mb:.2f} MB) ---")

    start_time = time.time()

    # Pass the filename as a URI query: ?file=filename.bin
    request = Message(code=PUT, payload=payload, uri=f"{SERVER_URI}?file={filename}")

    try:
        response = await context.request(request).response
        end_time = time.time()

        # Prevent division by zero
        duration = max(end_time - start_time, 0.001)

        print(f"Result: {response.code}")
        print(f"CoAP Time: {duration:.2f}s ({file_size_mb / duration:.2f} MB/s)")
    except Exception as e:
        print(f"Failed to send {filename}: {e}")


async def main():
    # Wait for the server container to spin up
    await asyncio.sleep(5)

    # Create the client context once and reuse it
    context = await Context.create_client_context()

    # Grab all .bin files in the directory
    files = [
        f for f in os.listdir(DATA_DIR)
        if os.path.isfile(os.path.join(DATA_DIR, f)) and f.endswith(".bin")
    ]

    for filename in files:
        await transfer_file(context, filename)

    print("\nTest complete. Keeping alive...")

    # Keep alive so Docker doesn't exit immediately
    while True:
        await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(main())