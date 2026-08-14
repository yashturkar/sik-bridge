"""Binary framing and streaming decode for the radio wire protocol."""

from __future__ import annotations

import binascii
import struct
from dataclasses import dataclass

MAGIC = b"\xaa\x55"
VERSION = 1
HEADER = struct.Struct(">2sBBBHH")
CRC = struct.Struct(">H")
HEADER_SIZE = HEADER.size
FRAME_OVERHEAD = HEADER_SIZE + CRC.size
DEFAULT_MAX_PAYLOAD = 256
HARD_MAX_PAYLOAD = 4096


def crc16(data: bytes) -> int:
    """Return CRC-16/CCITT-FALSE (poly 0x1021, init 0xffff)."""
    return binascii.crc_hqx(data, 0xFFFF)


@dataclass(frozen=True, slots=True)
class Frame:
    type: int
    flags: int
    seq: int
    payload: bytes = b""
    version: int = VERSION


def encode_frame(frame: Frame, max_payload: int = DEFAULT_MAX_PAYLOAD) -> bytes:
    if frame.version != VERSION:
        raise ValueError(f"unsupported protocol version: {frame.version}")
    if not 0 <= frame.type <= 0xFF or not 0 <= frame.flags <= 0xFF:
        raise ValueError("type and flags must fit in one byte")
    if not 0 <= frame.seq <= 0xFFFF:
        raise ValueError("sequence must fit in two bytes")
    if max_payload > HARD_MAX_PAYLOAD or max_payload < 1:
        raise ValueError(f"max_payload must be between 1 and {HARD_MAX_PAYLOAD}")
    if len(frame.payload) > max_payload:
        raise ValueError(f"payload exceeds configured maximum of {max_payload} bytes")
    header = HEADER.pack(MAGIC, frame.version, frame.type, frame.flags, frame.seq, len(frame.payload))
    protected = header[len(MAGIC) :] + frame.payload
    return header + frame.payload + CRC.pack(crc16(protected))


class FrameDecoder:
    """Incremental parser that resynchronizes after arbitrary damaged input."""

    def __init__(self, max_payload: int = DEFAULT_MAX_PAYLOAD) -> None:
        if not 1 <= max_payload <= HARD_MAX_PAYLOAD:
            raise ValueError(f"max_payload must be between 1 and {HARD_MAX_PAYLOAD}")
        self.max_payload = max_payload
        self._buffer = bytearray()
        self.crc_errors = 0
        self.length_errors = 0
        self.version_errors = 0
        self.discarded_bytes = 0

    def feed(self, data: bytes) -> list[Frame]:
        self._buffer.extend(data)
        frames: list[Frame] = []
        while True:
            start = self._buffer.find(MAGIC)
            if start < 0:
                keep = 1 if self._buffer.endswith(MAGIC[:1]) else 0
                self.discarded_bytes += len(self._buffer) - keep
                if keep:
                    self._buffer[:] = self._buffer[-1:]
                else:
                    self._buffer.clear()
                break
            if start:
                self.discarded_bytes += start
                del self._buffer[:start]
            if len(self._buffer) < HEADER_SIZE:
                break
            _, version, msg_type, flags, seq, length = HEADER.unpack_from(self._buffer)
            if version != VERSION:
                self.version_errors += 1
                self.discarded_bytes += 1
                del self._buffer[0]
                continue
            if length > self.max_payload:
                self.length_errors += 1
                self.discarded_bytes += 1
                del self._buffer[0]
                continue
            total = HEADER_SIZE + length + CRC.size
            if len(self._buffer) < total:
                break
            payload = bytes(self._buffer[HEADER_SIZE : HEADER_SIZE + length])
            expected = CRC.unpack_from(self._buffer, HEADER_SIZE + length)[0]
            actual = crc16(bytes(self._buffer[len(MAGIC) : HEADER_SIZE + length]))
            if actual != expected:
                self.crc_errors += 1
                self.discarded_bytes += 1
                del self._buffer[0]
                continue
            frames.append(Frame(msg_type, flags, seq, payload, version))
            del self._buffer[:total]
        return frames
