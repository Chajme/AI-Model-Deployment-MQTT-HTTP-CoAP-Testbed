import subprocess
import itertools
import os
import time

PROTOCOLS = ["coap"]
PROFILES = ["mobile"]
# PROFILES = ["mobile"]

BASE_ENV = os.environ.copy()

def run_experiment(protocol, profile):
    print(f"\n=== Running {protocol.upper()} | {profile} ===")

    env = BASE_ENV.copy()
    env["NETWORK_PROFILE"] = profile
    env["MEASUREMENT_SUFFIX"] = f"{protocol}_{profile}"

    try:
        # Start only selected protocol
        subprocess.run(
            [
                "docker", "compose",
                "-f", "docker-compose.automated.yaml",
                "--profile", protocol,
                "up",
                "--abort-on-container-exit"
            ],
            env=env,
            check=True
        )

    finally:
        # Always clean up
        subprocess.run(["docker", "compose", "down", "-v"], env=env)


if __name__ == "__main__":
    subprocess.run(["docker", "compose", "-f", "docker-compose.automated.yaml", "build"], check=True)

    for protocol, profile in itertools.product(PROTOCOLS, PROFILES):
        run_experiment(protocol, profile)