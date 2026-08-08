import subprocess
import sys

from common.file_manager import load_binary_files
from common.packet_capture import start_capture_run, stop_capture_run

def capture_mqtt(files):
    for qos_level in range(1, 3):

        for filename in files:

            start_capture_run(f"{filename}_{qos_level}")

            subprocess.run([
                "docker", "compose", "exec",
                "mqtt-client-a",
                "python",
                "-m",
                "protocols.MQTT.clients.client_a",
                "--file", filename,
                "--qos", str(qos_level),
            ], check=True)

            stop_capture_run()

def capture_mqtt_single_file(filename):
    for qos_level in range(1, 3):
        start_capture_run(f"{filename}_{qos_level}")

        subprocess.run([
            "docker", "compose", "exec",
            "mqtt-client-a",
            "python",
            "-m",
            "protocols.MQTT.clients.client_a",
            "--file", filename,
            "--qos", str(qos_level),
        ], check=True)

        stop_capture_run()

if __name__ == "__main__":

    # loaded_files = load_binary_files()

    # capture_mqtt(loaded_files)

    capture_mqtt_single_file("binary_file_1mb.bin")