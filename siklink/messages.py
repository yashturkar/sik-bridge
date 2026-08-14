"""Wire message types and MessagePack payload helpers."""

from __future__ import annotations

from enum import IntEnum
from typing import Any

import msgpack


class MessageType(IntEnum):
    HEARTBEAT = 0x01
    HEARTBEAT_ACK = 0x02
    USER_MESSAGE = 0x10
    ACK = 0x11
    NACK = 0x12
    LINK_STATUS = 0x20


ACK_REQUIRED = 0x01


def pack(value: Any) -> bytes:
    return msgpack.packb(value, use_bin_type=True)


def unpack(payload: bytes) -> Any:
    return msgpack.unpackb(payload, raw=False, strict_map_key=False)


def pack_user_message(topic: str, data: Any) -> bytes:
    if not isinstance(topic, str) or not topic:
        raise ValueError("topic must be a non-empty string")
    return pack({"topic": topic, "data": data})


def unpack_user_message(payload: bytes) -> dict[str, Any]:
    value = unpack(payload)
    if not isinstance(value, dict) or not isinstance(value.get("topic"), str) or not value["topic"]:
        raise ValueError("USER_MESSAGE payload must contain a non-empty string topic")
    if "data" not in value:
        raise ValueError("USER_MESSAGE payload must contain data")
    return {"topic": value["topic"], "data": value["data"]}
