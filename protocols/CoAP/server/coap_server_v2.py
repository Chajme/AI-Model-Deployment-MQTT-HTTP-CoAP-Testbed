import asyncio
import aiocoap.resource as resource
import aiocoap
import os

from output.integrity_checker import sha256

OUTPUT_DIR = "/app/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


class BinaryUploadResource(resource.Resource):
    async def render_put(self, request):
        filename          = "unknown.bin"
        expected_checksum = None

        for query in request.opt.uri_query:
            # FIX #8: split("=", 1) so filenames containing "=" are handled
            # correctly (e.g. base64-encoded names won't be truncated).
            if query.startswith("file="):
                filename = query.split("=", 1)[1]
            elif query.startswith("checksum="):
                expected_checksum = query.split("=", 1)[1]

        filepath = os.path.join(OUTPUT_DIR, filename)
        print(f"--- CoAP: Receiving '{filename}' ({len(request.payload)} bytes) ---")

        actual_checksum = sha256(request.payload)

        if expected_checksum and actual_checksum != expected_checksum:
            print(f"Checksum mismatch for {filename}!")
            return aiocoap.Message(
                code=aiocoap.BAD_REQUEST,
                # FIX #9: keep response payload small and consistent so it
                # doesn't inflate the client-side overhead estimate.
                payload=b"checksum mismatch",
            )

        with open(filepath, "wb") as f:
            f.write(request.payload)

        print(f"Saved '{filename}' successfully.")
        # FIX #9: minimal response payload keeps overhead accounting clean.
        return aiocoap.Message(code=aiocoap.CHANGED, payload=b"OK")


async def main():
    root = resource.Site()
    root.add_resource(["upload"], BinaryUploadResource())

    print("CoAP Server starting on UDP 5683 (Blockwise enabled)...")
    await aiocoap.Context.create_server_context(root, bind=("0.0.0.0", 5683))

    await asyncio.get_running_loop().create_future()


if __name__ == "__main__":
    asyncio.run(main())