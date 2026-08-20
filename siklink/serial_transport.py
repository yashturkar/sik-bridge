"""Reconnect-capable pyserial worker."""

from __future__ import annotations

import glob
import heapq
import itertools
import logging
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
        self._queue_size = queue_size
        self._queue: list[tuple[int, int, bytes, str | None]] = []
        self._queue_lock = threading.Lock()
        self._counter = itertools.count()
        self.dropped_frames = 0
        self.evicted_frames = 0
        self.coalesced_frames = 0
        self.discarded_frames = 0
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def queue_depth(self) -> int:
        with self._queue_lock:
            return len(self._queue)

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

    def send(self, data: bytes, priority: int = 10, replace_key: str | None = None) -> bool:
        if not self._connected.is_set():
            return False
        with self._queue_lock:
            if replace_key is not None:
                for index, item in enumerate(self._queue):
                    if item[3] == replace_key:
                        self._queue[index] = (priority, item[1], data, replace_key)
                        heapq.heapify(self._queue)
                        self.coalesced_frames += 1
                        return True
            item = (priority, next(self._counter), data, replace_key)
            if len(self._queue) < self._queue_size:
                heapq.heappush(self._queue, item)
                return True
            worst_priority = max(queued[0] for queued in self._queue)
            worst_index = min(
                (index for index, queued in enumerate(self._queue) if queued[0] == worst_priority),
                key=lambda index: self._queue[index][1],
            )
            if self._queue[worst_index][0] <= priority:
                self.dropped_frames += 1
                return False
            self._queue[worst_index] = item
            heapq.heapify(self._queue)
            self.evicted_frames += 1
            return True

    def discard_application(self) -> int:
        """Discard queued application frames while retaining protocol control frames."""
        with self._queue_lock:
            before = len(self._queue)
            self._queue = [item for item in self._queue if item[0] == 0]
            heapq.heapify(self._queue)
            discarded = before - len(self._queue)
            self.discarded_frames += discarded
            return discarded

    def _next_frame(self) -> bytes | None:
        with self._queue_lock:
            if not self._queue:
                return None
            return heapq.heappop(self._queue)[2]

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
            data = self._next_frame()
            if data is not None:
                port.write(data)
            waiting = port.in_waiting
            data = port.read(waiting or 1)
            if data:
                self.on_data(data)
