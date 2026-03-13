import asyncio
import time
from aiocoap import Message, Context, PUT


async def test_coap_communication():
    print("--- Phase 1: CoAP Handshake ---")
    context = await Context.create_client_context()

    payload = b"Protocol Test: Hello CoAP!"
    request = Message(code=PUT, payload=payload, uri="coap://coap-server/test")

    try:
        response = await context.request(request).response
        print(f"Result: {response.code}")
        print(f"Payload: {response.payload.decode('utf-8')}")
    except Exception as e:
        print(f"Failed to send CoAP message: {e}")


if __name__ == "__main__":
    time.sleep(5)
    asyncio.run(test_coap_communication())
    print("CoAP Test complete. Keeping alive...")
    time.sleep(30)