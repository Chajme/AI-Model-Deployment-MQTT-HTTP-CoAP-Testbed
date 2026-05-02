import asyncio
import aiocoap.resource as resource
import aiocoap
import os

from output.integrity_checker import compute_sha256_file, sha256

OUTPUT_DIR = "/app/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

class BinaryUploadResource(resource.Resource):
    async def render_put(self, request):
        # Extract filename from the query string (e.g., ?file=my_model.bin)
        filename = "unknown.bin"
        expected_checksum = None

        for query in request.opt.uri_query:
            if query.startswith("file="):
                filename = query.split("=")[1]
            elif query.startswith("checksum="):
                expected_checksum = query.split("=")[1]

        filepath = os.path.join(OUTPUT_DIR, filename)

        print(f"--- CoAP: Receiving file '{filename}' ({len(request.payload)} bytes) ---")

        actual_checksum = sha256(request.payload)

        if expected_checksum and actual_checksum != expected_checksum:
            print(f"Checksum mismatch for {filename}!")
            return aiocoap.Message(
                code=aiocoap.BAD_REQUEST,
                payload=b"Checksum mismatch"
            )

        with open(filepath, "wb") as f:
            f.write(request.payload)

        return aiocoap.Message(code=aiocoap.CHANGED, payload=f"Saved {filename}".encode())

async def main():
    root = resource.Site()
    # We bind exclusively to the exact '/upload' path
    root.add_resource(['upload'], BinaryUploadResource())

    print("CoAP Server starting on UDP 5683 (Blockwise enabled)...")
    await aiocoap.Context.create_server_context(root, bind=("0.0.0.0", 5683))

    await asyncio.get_running_loop().create_future()

if __name__ == "__main__":
    asyncio.run(main())