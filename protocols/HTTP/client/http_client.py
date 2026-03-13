import requests
import time

BASE_URL = "http://http-server"

def test_communication():
    print("--- Phase 1: Connection Check ---")
    try:
        response = requests.get(f"{BASE_URL}/")
        if response.status_code == 200:
            print(f"Success! Reached Nginx server. Status: {response.status_code}")
        else:
            print(f"Server reached, but got status: {response.status_code}")
    except Exception as e:
        print(f"Connection failed: {e}")
        return

    print("\n--- Phase 2: Basic Data Transfer ---")
    test_message = "Protocol Test: Hello Nginx!"
    upload_url = f"{BASE_URL}/upload/hello.txt"

    try:
        print(f"Sending text to {upload_url}...")
        put_response = requests.put(upload_url, data=test_message)

        if put_response.status_code in [201, 204]:
            print("Success! Data received and saved by Nginx.")
        else:
            print(f"Transfer failed. Status: {put_response.status_code}")
            print(f"Response Body: {put_response.text}")

    except Exception as e:
        print(f"Data transfer failed: {e}")


if __name__ == "__main__":
    time.sleep(5)
    test_communication()
    print("Test complete. Keeping container alive for 30s...")
    time.sleep(30)