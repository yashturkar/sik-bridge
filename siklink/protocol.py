"""Stateful radio protocol engine, independent of serial and asyncio."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from typing import Any

from .config import ProtocolConfig
from .framing import Frame, FrameDecoder, encode_frame
from .messages import (
    ACK_REQUIRED,
    MessageType,
    pack,
    pack_user_message,
    unpack,
    unpack_user_message,
)
from .metrics import LinkMetrics
from .reliability import DuplicateCache, PendingSend

LOG = logging.getLogger(__name__)
Emit = Callable[[bytes, int, str | None], bool]
Event = Callable[[str, dict[str, Any]], None]


class ProtocolEngine:
    """Transforms serial bytes into events and queued wire frames."""

    def __init__(
        self,
        config: ProtocolConfig,
        emit: Emit,
        event: Event,
        *,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        initial_seq: int | None = None,
    ) -> None:
        self.config = config
        self.emit = emit
        self.event = event
        self.clock = clock
        self.wall_clock = wall_clock
        self.decoder = FrameDecoder(config.max_payload)
        self.metrics = LinkMetrics()
        self.metrics.heartbeat_results = self.metrics.heartbeat_results.__class__(maxlen=config.loss_window)
        self._seq = random.SystemRandom().randrange(0x10000) if initial_seq is None else initial_seq & 0xFFFF
        self._pending: dict[int, PendingSend] = {}
        self._heartbeat_pending: dict[int, tuple[float, int]] = {}
        self._duplicates = DuplicateCache(config.duplicate_ttl, config.duplicate_cache_size)
        self._next_heartbeat = self.clock() + config.heartbeat_phase
        self._last_state: str | None = None
        self._last_rx_seq: int | None = None

    def _next_seq(self) -> int:
        seq = self._seq
        self._seq = (self._seq + 1) & 0xFFFF
        return seq

    def _send_frame(
        self, frame: Frame, priority: int = 10, replace_key: str | None = None,
    ) -> tuple[bool, bytes]:
        encoded = encode_frame(frame, self.config.max_payload)
        accepted = self.emit(encoded, priority, replace_key)
        if accepted:
            self.metrics.tx_frames += 1
            self.metrics.tx_bytes += len(encoded)
        return accepted, encoded

    def send_user(
        self, topic: str, data: Any, reliable: bool = False, context: Any = None,
        *, priority: int = 10, latest: bool = False,
    ) -> int:
        if reliable and latest:
            raise ValueError("reliable messages cannot use latest-value replacement")
        seq = self._next_seq()
        flags = ACK_REQUIRED if reliable else 0
        frame = Frame(MessageType.USER_MESSAGE, flags, seq, pack_user_message(topic, data))
        owner = context[0] if isinstance(context, tuple) and context else "anonymous"
        accepted, encoded = self._send_frame(
            frame, priority=priority,
            replace_key=f"user:{owner}:{topic}" if latest else None,
        )
        if not accepted:
            raise BufferError("transmit queue is full")
        if reliable:
            self._pending[seq] = PendingSend(
                encoded,
                self.clock() + self.config.ack_timeout,
                self.config.max_retries,
                context,
                priority,
            )
        else:
            self.event("send_result", {"seq": seq, "ok": True, "reliable": False, "context": context})
        return seq

    def receive(self, data: bytes) -> None:
        before_crc = self.decoder.crc_errors
        before_parser = self.decoder.length_errors + self.decoder.version_errors
        for frame in self.decoder.feed(data):
            now = self.clock()
            self.metrics.rx_frames += 1
            self.metrics.rx_bytes += len(frame.payload)
            self.metrics.last_valid_rx = now
            self._last_rx_seq = frame.seq
            self._handle_frame(frame, now)
        self.metrics.crc_errors += self.decoder.crc_errors - before_crc
        parser_now = self.decoder.length_errors + self.decoder.version_errors
        self.metrics.parser_errors += parser_now - before_parser

    def _handle_frame(self, frame: Frame, now: float) -> None:
        try:
            msg_type = MessageType(frame.type)
        except ValueError:
            if frame.flags & ACK_REQUIRED:
                self._send_control(MessageType.NACK, {"seq": frame.seq, "error": "unknown_type"})
            return
        if msg_type == MessageType.USER_MESSAGE:
            self._handle_user(frame, now)
        elif msg_type == MessageType.ACK:
            self._handle_ack(frame, True)
        elif msg_type == MessageType.NACK:
            self._handle_ack(frame, False)
        elif msg_type == MessageType.HEARTBEAT:
            self._handle_heartbeat(frame)
        elif msg_type == MessageType.HEARTBEAT_ACK:
            self._handle_heartbeat_ack(frame, now)

    def _handle_user(self, frame: Frame, now: float) -> None:
        reliable = bool(frame.flags & ACK_REQUIRED)
        try:
            message = unpack_user_message(frame.payload)
        except (TypeError, ValueError) as exc:
            if reliable:
                self._send_control(MessageType.NACK, {"seq": frame.seq, "error": "invalid_payload"})
            LOG.debug("invalid user payload: %s", exc)
            return
        if reliable and self._duplicates.contains_or_add(frame.seq, now):
            self.metrics.duplicates += 1
            self._send_control(MessageType.ACK, {"seq": frame.seq})
            return
        if reliable:
            self._send_control(MessageType.ACK, {"seq": frame.seq})
        self.event("message", {**message, "seq": frame.seq, "reliable": reliable})

    def _handle_ack(self, frame: Frame, ok: bool) -> None:
        try:
            payload = unpack(frame.payload)
            acked = int(payload["seq"])
        except (KeyError, TypeError, ValueError):
            self.metrics.parser_errors += 1
            return
        pending = self._pending.pop(acked, None)
        if pending is not None:
            self.event(
                "send_result",
                {"seq": acked, "ok": ok, "reliable": True, "context": pending.request_context,
                 "error": None if ok else payload.get("error", "nack")},
            )

    def _handle_heartbeat(self, frame: Frame) -> None:
        try:
            payload = unpack(frame.payload)
            timestamp_ms = int(payload["timestamp_ms"])
        except (KeyError, TypeError, ValueError):
            self.metrics.parser_errors += 1
            return
        self._send_control(MessageType.HEARTBEAT_ACK, {"seq": frame.seq, "timestamp_ms": timestamp_ms})

    def _handle_heartbeat_ack(self, frame: Frame, now: float) -> None:
        try:
            payload = unpack(frame.payload)
            seq = int(payload["seq"])
        except (KeyError, TypeError, ValueError):
            self.metrics.parser_errors += 1
            return
        pending = self._heartbeat_pending.pop(seq, None)
        if pending is None:
            return
        sent_at, _ = pending
        self.metrics.rtt_ms = (now - sent_at) * 1000
        self._record_heartbeat(True)

    def _record_heartbeat(self, success: bool) -> None:
        self.metrics.record_heartbeat(
            success,
            degraded_rtt_ms=self.config.degraded_rtt_ms,
            degraded_loss=self.config.degraded_loss,
            minimum_samples=self.config.minimum_health_samples,
            enter_samples=self.config.degraded_enter_samples,
            exit_samples=self.config.degraded_exit_samples,
        )

    def _send_control(self, msg_type: MessageType, payload: dict[str, Any]) -> None:
        self._send_frame(Frame(msg_type, 0, self._next_seq(), pack(payload)), priority=0)

    def tick(self) -> None:
        now = self.clock()
        if now >= self._next_heartbeat:
            self._expire_heartbeats(now)
            seq = self._next_seq()
            timestamp_ms = int(self.wall_clock() * 1000)
            snapshot = self.status(now)
            payload = pack({
                "uptime_ms": snapshot["uptime_ms"],
                "timestamp_ms": timestamp_ms,
                "last_rx_seq": self._last_rx_seq,
                "tx_frames": self.metrics.tx_frames,
                "rx_frames": self.metrics.rx_frames,
                "crc_errors": self.metrics.crc_errors,
            })
            accepted, _ = self._send_frame(Frame(MessageType.HEARTBEAT, 0, seq, payload), priority=0)
            if accepted:
                self._heartbeat_pending[seq] = (now, timestamp_ms)
            self._next_heartbeat = now + self.config.heartbeat_interval
        for seq, pending in list(self._pending.items()):
            if now < pending.deadline:
                continue
            if pending.retries_left > 0:
                accepted = self.emit(pending.frame, min(5, pending.priority), None)
                pending.retries_left -= 1
                pending.deadline = now + self.config.ack_timeout
                if accepted:
                    self.metrics.retries += 1
                    self.metrics.tx_frames += 1
                    self.metrics.tx_bytes += len(pending.frame)
            else:
                del self._pending[seq]
                self.metrics.reliable_timeouts += 1
                self.event("send_result", {"seq": seq, "ok": False, "reliable": True,
                                             "context": pending.request_context, "error": "timeout"})
        state = self.status(now)["state"]
        if state != self._last_state:
            if state == "DISCONNECTED":
                self.fail_pending("disconnected")
            self._last_state = state
            self.event("link_status", self.status(now))

    def fail_pending(self, error: str) -> None:
        for seq, pending in list(self._pending.items()):
            self.event("send_result", {
                "seq": seq, "ok": False, "reliable": True,
                "context": pending.request_context, "error": error,
            })
        self._pending.clear()

    def reset_link(self) -> None:
        """Forget state tied to a serial stream that is being reopened."""
        self.decoder = FrameDecoder(self.config.max_payload)
        self._heartbeat_pending.clear()
        self.metrics.reset_link_quality()
        self.metrics.last_valid_rx = None
        self._last_rx_seq = None
        self._last_state = None

    def _expire_heartbeats(self, now: float) -> None:
        cutoff = now - self.config.heartbeat_reply_timeout
        expired = [seq for seq, (sent, _) in self._heartbeat_pending.items() if sent <= cutoff]
        for seq in expired:
            del self._heartbeat_pending[seq]
            self._record_heartbeat(False)

    def status(self, now: float | None = None) -> dict[str, Any]:
        return self.metrics.snapshot(
            self.clock() if now is None else now,
            self.config.disconnect_after,
            self.config.degraded_rtt_ms,
            self.config.degraded_loss,
        )
