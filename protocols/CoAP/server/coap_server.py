import asyncio
import aiocoap.resource as resource
import aiocoap


class BasicResource(resource.Resource):
    async def render_put(self, request):
        payload = request.payload.decode('utf-8')
        print(f"--- CoAP Server Received: {payload} ---")

        return aiocoap.Message(code=aiocoap.CHANGED, payload=b"ACK: Received")


async def main():
    root = resource.Site()
    root.add_resource(['test'], BasicResource())

    print("CoAP Server starting on UDP 5683...")
    await aiocoap.Context.create_server_context(root, bind=("0.0.0.0", 5683))

    await asyncio.get_running_loop().create_future()


if __name__ == "__main__":
    asyncio.run(main())