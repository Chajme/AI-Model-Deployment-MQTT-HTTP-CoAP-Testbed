import time
import paho.mqtt.client as mqtt

BROKER = "mosquitto-broker"
TOPIC = "paho/temperature"

client = mqtt.Client()

client.connect(BROKER, 1883, 60)

client.loop_start()

message = 0

while True:
    print(f"Sending {message}")
    client.publish(TOPIC, message)
    message += 1
    time.sleep(1)