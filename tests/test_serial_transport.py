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


def test_control_evicts_bulk_and_latest_replaces_queued_value():
    transport = SerialTransport(SerialConfig(), 2, lambda _: None, lambda *_: None)
    transport._connected.set()
    assert transport.send(b"old-state", priority=10, replace_key="state")
    assert transport.send(b"bulk", priority=20)
    assert transport.send(b"new-state", priority=10, replace_key="state")
    assert transport.send(b"stop", priority=2)
    assert transport._next_frame() == b"stop"
    assert transport._next_frame() == b"new-state"


def test_control_evicts_oldest_frame_among_equal_low_priorities():
    transport = SerialTransport(SerialConfig(), 3, lambda _: None, lambda *_: None)
    transport._connected.set()
    assert transport.send(b"old-bulk", priority=20)
    assert transport.send(b"new-bulk", priority=20)
    assert transport.send(b"normal", priority=10)
    assert transport.send(b"stop", priority=2)
    assert transport._next_frame() == b"stop"
    assert transport._next_frame() == b"normal"
    assert transport._next_frame() == b"new-bulk"


def test_requested_reconnect_closes_and_reopens_port():
    master, slave = pty.openpty()
    states = []
    connected_twice = threading.Event()
    transport = SerialTransport(
        SerialConfig(
            device=os.ttyname(slave), read_timeout=0.01,
            reconnect_initial=0.01, reconnect_max=0.02,
        ),
        8,
        lambda _: None,
        lambda state, _device, error: (
            states.append((state, error)),
            connected_twice.set() if state and sum(value for value, _ in states) >= 2 else None,
        ),
    )
    try:
        transport.start()
        deadline = time.monotonic() + 1
        while not transport._connected.is_set() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert transport.request_reconnect("silent USB stream")
        assert connected_twice.wait(1)
        assert (False, "silent USB stream") in states
        assert transport.forced_reconnects == 1
    finally:
        transport.stop()
        os.close(master)
        os.close(slave)
