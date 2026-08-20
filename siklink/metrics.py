"""Link counters and health-state calculation."""

from __future__ import annotations

import time
from collections import deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class LinkState(str, Enum):
    CONNECTED = "CONNECTED"
    DEGRADED = "DEGRADED"
    DISCONNECTED = "DISCONNECTED"


@dataclass(slots=True)
class LinkMetrics:
    started_at: float = field(default_factory=time.monotonic)
    last_valid_rx: float | None = None
    serial_connected: bool = False
    serial_device: str | None = None
    rtt_ms: float | None = None
    tx_frames: int = 0
    rx_frames: int = 0
    tx_bytes: int = 0
    rx_bytes: int = 0
    crc_errors: int = 0
    parser_errors: int = 0
    retries: int = 0
    duplicates: int = 0
    reliable_timeouts: int = 0
    serial_disconnects: int = 0
    serial_recoveries: int = 0
    usb_resets: int = 0
    usb_reset_failures: int = 0
    last_serial_error: str | None = None
    last_usb_reset_error: str | None = None
    tx_queue_depth: int = 0
    tx_dropped_frames: int = 0
    tx_evicted_frames: int = 0
    tx_coalesced_frames: int = 0
    tx_discarded_frames: int = 0
    heartbeat_results: deque[bool] = field(default_factory=lambda: deque(maxlen=30), repr=False)
    degraded_latched: bool = False
    health_bad_streak: int = 0
    health_good_streak: int = 0

    @property
    def heartbeat_loss(self) -> float:
        if not self.heartbeat_results:
            return 0.0
        return 1.0 - (sum(self.heartbeat_results) / len(self.heartbeat_results))

    def record_heartbeat(
        self,
        success: bool,
        *,
        degraded_rtt_ms: float,
        degraded_loss: float,
        minimum_samples: int,
        enter_samples: int,
        exit_samples: int,
    ) -> None:
        """Record one completed heartbeat and apply sample-based quality hysteresis."""
        self.heartbeat_results.append(success)
        enough_samples = len(self.heartbeat_results) >= minimum_samples
        unhealthy = enough_samples and (
            not success
            or (self.rtt_ms is not None and self.rtt_ms > degraded_rtt_ms)
            or self.heartbeat_loss > degraded_loss
        )
        if unhealthy:
            self.health_bad_streak += 1
            self.health_good_streak = 0
            if self.health_bad_streak >= enter_samples:
                self.degraded_latched = True
        else:
            self.health_good_streak += 1
            self.health_bad_streak = 0
            if self.degraded_latched and self.health_good_streak >= exit_samples:
                self.degraded_latched = False

    def reset_link_quality(self) -> None:
        self.heartbeat_results.clear()
        self.rtt_ms = None
        self.degraded_latched = False
        self.health_bad_streak = 0
        self.health_good_streak = 0

    def state(self, now: float, disconnect_after: float, _degraded_rtt_ms: float, _degraded_loss: float) -> LinkState:
        if self.last_valid_rx is None or now - self.last_valid_rx >= disconnect_after:
            return LinkState.DISCONNECTED
        if self.degraded_latched:
            return LinkState.DEGRADED
        return LinkState.CONNECTED

    def snapshot(
        self,
        now: float,
        disconnect_after: float,
        degraded_rtt_ms: float,
        degraded_loss: float,
    ) -> dict[str, Any]:
        values = asdict(self)
        values.pop("started_at", None)
        values.pop("last_valid_rx", None)
        values.pop("heartbeat_results", None)
        values.update(
            uptime_ms=int((now - self.started_at) * 1000),
            last_rx_age_s=None if self.last_valid_rx is None else max(0.0, now - self.last_valid_rx),
            heartbeat_loss=self.heartbeat_loss,
            heartbeat_samples=len(self.heartbeat_results),
            state=self.state(now, disconnect_after, degraded_rtt_ms, degraded_loss).value,
        )
        return values
