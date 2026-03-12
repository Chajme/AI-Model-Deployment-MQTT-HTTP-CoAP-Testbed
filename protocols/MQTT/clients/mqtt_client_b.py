import paho.mqtt.client as mqtt

BROKER = "mosquitto-broker"
TOPIC = "paho/temperature"


def on_connect(client, userdata, flags, rc):
    print("Connected to broker")
    client.subscribe(TOPIC)


def on_message(client, userdata, msg):
    print(f"Received: {msg.payload.decode()}")


client = mqtt.Client()

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, 1883, 60)

client.loop_forever()