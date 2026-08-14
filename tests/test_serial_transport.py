import os
import pty
import select
import threading
import time

from siklink.config import SerialConfig
from siklink.serial_transport import SerialTransport


def test_serial_transport_over_pty():
    master, slave = pty.openpty()
    slave_name = os.ttyname(slave)
    received = bytearray()
    connected = threading.Event()
    transport = SerialTransport(
        SerialConfig(device=slave_name, baud=57600, read_timeout=0.02),
        8,
        lambda data: received.extend(data),
        lambda state, *_: connected.set() if state else None,
    )
    try:
        transport.start()
        assert connected.wait(1)
        os.write(master, b"radio-to-host")
        deadline = time.monotonic() + 1
        while bytes(received) != b"radio-to-host" and time.monotonic() < deadline:
            time.sleep(0.01)
        assert bytes(received) == b"radio-to-host"

        assert transport.send(b"host-to-radio")
        readable, _, _ = select.select([master], [], [], 1)
        assert readable
        assert os.read(master, 1024) == b"host-to-radio"
    finally:
        transport.stop()
        os.close(master)
        os.close(slave)
