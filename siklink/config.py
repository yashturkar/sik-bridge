"""Validated YAML configuration."""

from __future__ import annotations

from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any, TypeVar

import yaml

from .framing import HARD_MAX_PAYLOAD


@dataclass(slots=True)
class SerialConfig:
    device: str = "auto"
    match: str = ""
    baud: int = 57600
    read_timeout: float = 0.1
    reconnect_initial: float = 0.5
    reconnect_max: float = 5.0
    rf_silence_reopen_after: float = 15.0
    usb_reset_after_reopens: int = 1
    usb_reset_settle_seconds: float = 1.0


@dataclass(slots=True)
class ProtocolConfig:
    max_payload: int = 256
    heartbeat_interval: float = 1.0
    heartbeat_phase: float = 0.0
    heartbeat_reply_timeout: float = 2.0
    disconnect_after: float = 3.0
    degraded_rtt_ms: float = 500.0
    degraded_loss: float = 0.20
    loss_window: int = 30
    minimum_health_samples: int = 5
    degraded_enter_samples: int = 3
    degraded_exit_samples: int = 5
    ack_timeout: float = 0.3
    max_retries: int = 3
    duplicate_ttl: float = 10.0
    duplicate_cache_size: int = 512
    tx_queue_size: int = 256


@dataclass(slots=True)
class SocketConfig:
    path: str = "/run/sik-link/sik-link.sock"
    mode: int = 0o660
    max_message: int = 1_048_576


@dataclass(slots=True)
class AppConfig:
    node_name: str
    serial: SerialConfig
    protocol: ProtocolConfig
    socket: SocketConfig
    log_level: str = "INFO"


T = TypeVar("T")


def _section(cls: type[T], raw: dict[str, Any], name: str) -> T:
    allowed = {field.name for field in fields(cls)}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(f"unknown {name} configuration keys: {', '.join(sorted(unknown))}")
    return cls(**raw)


def load_config(path: str | Path) -> AppConfig:
    with Path(path).open("rb") as stream:
        raw = yaml.safe_load(stream) or {}
    if not isinstance(raw, dict):
        raise TypeError("configuration root must be a mapping")
    allowed = {"node_name", "serial", "protocol", "socket", "log_level"}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError(f"unknown configuration keys: {', '.join(sorted(unknown))}")
    node_name = raw.get("node_name")
    if not isinstance(node_name, str) or not node_name:
        raise ValueError("node_name is required")
    serial = _section(SerialConfig, raw.get("serial", {}), "serial")
    protocol = _section(ProtocolConfig, raw.get("protocol", {}), "protocol")
    socket = _section(SocketConfig, raw.get("socket", {}), "socket")
    if not 1 <= protocol.max_payload <= HARD_MAX_PAYLOAD:
        raise ValueError(f"protocol.max_payload must be between 1 and {HARD_MAX_PAYLOAD}")
    if (
        serial.baud <= 0
        or serial.read_timeout <= 0
        or serial.reconnect_initial <= 0
        or serial.reconnect_max <= 0
        or serial.rf_silence_reopen_after < 0
        or serial.usb_reset_after_reopens < 0
        or serial.usb_reset_settle_seconds < 0
        or protocol.heartbeat_interval <= 0
        or protocol.heartbeat_phase < 0
        or protocol.heartbeat_reply_timeout <= protocol.heartbeat_interval
        or protocol.disconnect_after <= 0
        or protocol.loss_window <= 0
        or protocol.minimum_health_samples <= 0
        or protocol.degraded_enter_samples <= 0
        or protocol.degraded_exit_samples <= 0
    ):
        raise ValueError("serial and protocol timing/count values are invalid")
    if 0 < serial.rf_silence_reopen_after < protocol.disconnect_after:
        raise ValueError("serial.rf_silence_reopen_after must be zero or at least protocol.disconnect_after")
    if not 0 <= protocol.degraded_loss <= 1:
        raise ValueError("protocol.degraded_loss must be between 0 and 1")
    if protocol.minimum_health_samples > protocol.loss_window:
        raise ValueError("protocol.minimum_health_samples must not exceed loss_window")
    return AppConfig(node_name, serial, protocol, socket, str(raw.get("log_level", "INFO")).upper())
