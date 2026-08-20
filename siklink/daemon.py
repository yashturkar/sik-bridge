"""Async daemon combining the radio protocol with a local Unix socket."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import msgpack

from .config import AppConfig
from .protocol import ProtocolEngine
from .serial_transport import SerialTransport

LOG = logging.getLogger(__name__)
LENGTH = struct.Struct(">I")


async def read_packet(reader: asyncio.StreamReader, max_message: int) -> dict[str, Any]:
    length = LENGTH.unpack(await reader.readexactly(LENGTH.size))[0]
    if length == 0 or length > max_message:
        raise ValueError(f"invalid local message length: {length}")
    value = msgpack.unpackb(await reader.readexactly(length), raw=False, strict_map_key=False)
    if not isinstance(value, dict):
        raise TypeError("local message must be a map")
    return value


async def write_packet(writer: asyncio.StreamWriter, value: dict[str, Any]) -> None:
    payload = msgpack.packb(value, use_bin_type=True)
    writer.write(LENGTH.pack(len(payload)) + payload)
    await writer.drain()


@dataclass(eq=False)
class LocalPeer:
    peer_id: int
    writer: asyncio.StreamWriter
    topics: set[str] = field(default_factory=set)
    status_events: bool = False
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def send(self, value: dict[str, Any]) -> None:
        async with self.write_lock:
            await write_packet(self.writer, value)


class SikLinkDaemon:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.loop = asyncio.get_running_loop()
        self.peers: dict[int, LocalPeer] = {}
        self._peer_counter = 0
        self._server: asyncio.AbstractServer | None = None
        self._stop = asyncio.Event()
        self.transport = SerialTransport(
            config.serial,
            config.protocol.tx_queue_size,
            lambda data: self.loop.call_soon_threadsafe(self.engine.receive, data),
            lambda connected, device, error: self.loop.call_soon_threadsafe(
                self._serial_state, connected, device, error
            ),
        )
        self.engine = ProtocolEngine(config.protocol, self._emit, self._event)

    def _emit(self, data: bytes, priority: int, replace_key: str | None = None) -> bool:
        accepted = self.transport.send(data, priority, replace_key)
        self._sync_transport_metrics()
        return accepted

    def _sync_transport_metrics(self) -> None:
        metrics = self.engine.metrics
        metrics.tx_queue_depth = self.transport.queue_depth
        metrics.tx_dropped_frames = self.transport.dropped_frames
        metrics.tx_evicted_frames = self.transport.evicted_frames
        metrics.tx_coalesced_frames = self.transport.coalesced_frames
        metrics.tx_discarded_frames = self.transport.discarded_frames

    def _serial_state(self, connected: bool, device: str | None, error: str | None) -> None:
        previous = self.engine.metrics.serial_connected
        self.engine.metrics.serial_connected = connected
        self.engine.metrics.serial_device = device
        if previous and not connected:
            self.engine.metrics.serial_disconnects += 1
            self.transport.discard_application()
            self.engine.fail_pending("serial disconnected")
        self._event("serial_status", {"connected": connected, "device": device, "error": error})

    def _event(self, name: str, payload: dict[str, Any]) -> None:
        if name == "link_status" and payload.get("state") == "DISCONNECTED":
            self.transport.discard_application()
        context = payload.pop("context", None)
        if name == "send_result" and context is not None:
            peer_id, request_id = context
            peer = self.peers.get(peer_id)
            if peer:
                asyncio.create_task(peer.send({"id": request_id, **payload}))
            return
        asyncio.create_task(self._broadcast(name, payload))

    async def _broadcast(self, name: str, payload: dict[str, Any]) -> None:
        topic = payload.get("topic")
        for peer in list(self.peers.values()):
            interested = (name == "message" and ("*" in peer.topics or topic in peer.topics)) or (
                name in {"link_status", "serial_status"} and peer.status_events
            )
            if interested:
                try:
                    await peer.send({"event": name, **payload})
                except (ConnectionError, BrokenPipeError):
                    pass

    async def run(self) -> None:
        socket_path = Path(self.config.socket.path)
        socket_path.parent.mkdir(parents=True, exist_ok=True)
        if socket_path.exists():
            socket_path.unlink()
        self._server = await asyncio.start_unix_server(self._handle_peer, path=str(socket_path))
        os.chmod(socket_path, self.config.socket.mode)
        self.transport.start()
        ticker = asyncio.create_task(self._ticker())
        LOG.info("%s listening on %s", self.config.node_name, socket_path)
        try:
            await self._stop.wait()
        finally:
            ticker.cancel()
            self.transport.stop()
            self._server.close()
            await self._server.wait_closed()
            for peer in list(self.peers.values()):
                peer.writer.close()
            try:
                socket_path.unlink()
            except FileNotFoundError:
                pass

    def stop(self) -> None:
        self._stop.set()

    async def _ticker(self) -> None:
        while True:
            self._sync_transport_metrics()
            self.engine.tick()
            await asyncio.sleep(0.05)

    async def _handle_peer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._peer_counter += 1
        peer = LocalPeer(self._peer_counter, writer)
        self.peers[peer.peer_id] = peer
        try:
            while True:
                request = await read_packet(reader, self.config.socket.max_message)
                await self._handle_request(peer, request)
        except (asyncio.IncompleteReadError, ConnectionError, BrokenPipeError):
            pass
        except Exception as exc:  # noqa: BLE001 - isolate malformed or misbehaving local clients
            LOG.warning("local client error: %s", exc)
            try:
                await peer.send({"error": str(exc)})
            except Exception:
                LOG.debug("could not report local client error", exc_info=True)
        finally:
            self.peers.pop(peer.peer_id, None)
            writer.close()
            await writer.wait_closed()

    async def _handle_request(self, peer: LocalPeer, request: dict[str, Any]) -> None:
        request_id = request.get("id")
        op = request.get("op")
        if request_id is None or not isinstance(op, str):
            raise ValueError("request requires id and op")
        if op == "status":
            await peer.send({"id": request_id, "ok": True, "status": self.engine.status()})
        elif op == "subscribe":
            topics = request.get("topics", ["*"])
            if not isinstance(topics, list) or not all(isinstance(x, str) for x in topics):
                raise ValueError("topics must be a list of strings")
            peer.topics = set(topics)
            peer.status_events = bool(request.get("status", True))
            await peer.send({"id": request_id, "ok": True})
        elif op == "send":
            try:
                traffic_class = request.get("traffic_class", "normal")
                priorities = {"control": 2, "normal": 10, "bulk": 20}
                if traffic_class not in priorities:
                    raise ValueError("traffic_class must be control, normal, or bulk")
                if self.engine.status().get("state") == "DISCONNECTED":
                    raise BufferError("RF link is disconnected")
                self.engine.send_user(
                    request["topic"], request.get("data"), bool(request.get("reliable", False)),
                    (peer.peer_id, request_id), priority=priorities[traffic_class],
                    latest=bool(request.get("latest", False)),
                )
            except (BufferError, KeyError, TypeError, ValueError) as exc:
                await peer.send({"id": request_id, "ok": False, "error": str(exc)})
        else:
            await peer.send({"id": request_id, "ok": False, "error": f"unknown operation: {op}"})


async def run_daemon(config: AppConfig) -> None:
    daemon = SikLinkDaemon(config)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, daemon.stop)
        except NotImplementedError:
            pass
    await daemon.run()
