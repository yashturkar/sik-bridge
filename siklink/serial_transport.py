"""Reconnect-capable pyserial worker."""

from __future__ import annotations

import glob
import itertools
import logging
import queue
import threading
from collections.abc import Callable
from pathlib import Path

import serial

from .config import SerialConfig

LOG = logging.getLogger(__name__)


def discover_devices(pattern: str = "") -> list[str]:
    candidates = sorted(glob.glob("/dev/serial/by-id/*"))
    if pattern:
        candidates = [path for path in candidates if pattern.lower() in Path(path).name.lower()]
    return candidates


def resolve_device(config: SerialConfig) -> str:
    if config.device != "auto":
        return config.device
    candidates = discover_devices(config.match)
    if not candidates:
        raise FileNotFoundError("no matching serial device found under /dev/serial/by-id")
    if len(candidates) > 1:
        raise RuntimeError("multiple matching serial devices found: " + ", ".join(candidates))
    return candidates[0]


class SerialTransport:
    def __init__(
        self,
        config: SerialConfig,
        queue_size: int,
        on_data: Callable[[bytes], None],
        on_state: Callable[[bool, str | None, str | None], None],
    ) -> None:
        self.config = config
        self.on_data = on_data
        self.on_state = on_state
        self._queue: queue.PriorityQueue[tuple[int, int, bytes]] = queue.PriorityQueue(queue_size)
        self._counter = itertools.count()
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="sik-serial", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=max(2.0, self.config.read_timeout + 1.0))

    def send(self, data: bytes, priority: int = 10) -> bool:
        if not self._connected.is_set():
            return False
        try:
            self._queue.put_nowait((priority, next(self._counter), data))
            return True
        except queue.Full:
            return False

    def _run(self) -> None:
        delay = self.config.reconnect_initial
        while not self._stop.is_set():
            try:
                device = resolve_device(self.config)
                with serial.Serial(device, self.config.baud, timeout=self.config.read_timeout, write_timeout=1.0) as port:
                    LOG.info("serial connected: %s at %d baud", device, self.config.baud)
                    self._connected.set()
                    self.on_state(True, device, None)
                    delay = self.config.reconnect_initial
                    self._connected_loop(port)
            except (OSError, RuntimeError, serial.SerialException) as exc:
                self._connected.clear()
                LOG.warning("serial unavailable: %s", exc)
                self.on_state(False, None, str(exc))
            if not self._stop.wait(delay):
                delay = min(self.config.reconnect_max, max(self.config.reconnect_initial, delay * 2))

    def _connected_loop(self, port: serial.Serial) -> None:
        while not self._stop.is_set():
            try:
                while True:
                    _, _, data = self._queue.get_nowait()
                    port.write(data)
            except queue.Empty:
                pass
            waiting = port.in_waiting
            data = port.read(waiting or 1)
            if data:
                self.on_data(data)
