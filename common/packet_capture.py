import subprocess
import time


def start_capture_run(label: str, service: str = "mqtt-capture", iface: str = "eth0") -> str:
    """Start tcpdump inside the (already-running) capture container for one run."""
    outfile = f"/pcap/mqtt_{label}.pcap"
    subprocess.run(
        [
            "docker", "compose", "exec", "-d", service,
            "tcpdump", "-U", "-i", iface, "-s", "0", "-w", outfile,
        ],
        check=True,
    )
    _wait_until_ready(service, outfile)
    return outfile


def _wait_until_ready(service: str, outfile: str, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = subprocess.run(
            ["docker", "compose", "exec", service, "test", "-s", outfile],
        )
        if result.returncode == 0:
            return
        time.sleep(0.1)
    raise RuntimeError(f"tcpdump never started writing {outfile} inside {service}")


def stop_capture_run(service: str = "mqtt-capture") -> None:
    """Stop tcpdump inside the capture container (SIGTERM -> clean pcap close)."""
    subprocess.run(["docker", "compose", "exec", service, "pkill", "-TERM", "tcpdump"], check=True)


if __name__ == "__main__":
    start_capture_run("5mb_run1")
    # ... send the 5MB file here ...
    stop_capture_run()

    start_capture_run("50mb_run1")
    # ... send the 50MB file here ...
    stop_capture_run()