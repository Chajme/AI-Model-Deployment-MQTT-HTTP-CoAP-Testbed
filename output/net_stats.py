# output/net_stats.py  — shared by all three protocol clients

def get_iface_bytes(iface: str = "eth0") -> tuple[int, int]:
    """
    Read cumulative RX / TX byte counters from the kernel.
    Returns (rx_bytes, tx_bytes).
    Column layout from /proc/net/dev:
      face | rx: bytes packets errs drop ... | tx: bytes packets errs drop ...
    """
    with open("/proc/net/dev") as f:
        for line in f:
            if iface + ":" in line:
                parts = line.split()
                # parts[0] = "eth0:", parts[1] = rx_bytes, parts[9] = tx_bytes
                return int(parts[1]), int(parts[9])
    raise RuntimeError(f"Interface {iface!r} not found in /proc/net/dev")


class WireSnapshot:
    """Context manager that measures total bytes sent+received over a transfer."""

    def __init__(self, iface: str = "eth0"):
        self.iface = iface
        self._rx0 = self._tx0 = 0
        self.rx_bytes = self.tx_bytes = self.total_bytes = 0

    def __enter__(self):
        self._rx0, self._tx0 = get_iface_bytes(self.iface)
        return self

    def __exit__(self, *_):
        rx1, tx1 = get_iface_bytes(self.iface)
        self.rx_bytes  = rx1 - self._rx0
        self.tx_bytes  = tx1 - self._tx0
        self.total_bytes = self.rx_bytes + self.tx_bytes