"""Async client for the daemon's local MessagePack socket API."""

from __future__ import annotations

import asyncio
import itertools
from typing import Any

from typing_extensions import Self

from .daemon import read_packet, write_packet


class SikLinkClient:
    def __init__(self, socket_path: str = "/run/sik-link/sik-link.sock", max_message: int = 1_048_576) -> None:
        self.socket_path = socket_path
        self.max_message = max_message
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._ids = itertools.count(1)
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._events: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._reader_task: asyncio.Task[None] | None = None
        self._write_lock = asyncio.Lock()

    async def connect(self) -> Self:
        self.reader, self.writer = await asyncio.open_unix_connection(self.socket_path)
        self._reader_task = asyncio.create_task(self._read_loop())
        return self

    async def close(self) -> None:
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
        if self._reader_task:
            self._reader_task.cancel()
        self.reader = None
        self.writer = None

    async def __aenter__(self) -> Self:
        return await self.connect()

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def _request(self, op: str, **values: Any) -> dict[str, Any]:
        if self.writer is None:
            raise RuntimeError("client is not connected")
        request_id = next(self._ids)
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        async with self._write_lock:
            await write_packet(self.writer, {"id": request_id, "op": op, **values})
        return await future

    async def send(self, topic: str, data: Any, reliable: bool = False) -> dict[str, Any]:
        return await self._request("send", topic=topic, data=data, reliable=reliable)

    async def status(self) -> dict[str, Any]:
        response = await self._request("status")
        if not response.get("ok"):
            raise RuntimeError(response.get("error", "status failed"))
        return response["status"]

    async def subscribe(self, topics: list[str] | None = None, status: bool = True) -> None:
        response = await self._request("subscribe", topics=topics or ["*"], status=status)
        if not response.get("ok"):
            raise RuntimeError(response.get("error", "subscribe failed"))

    async def events(self):
        while True:
            yield await self._events.get()

    async def _read_loop(self) -> None:
        assert self.reader is not None
        try:
            while True:
                packet = await read_packet(self.reader, self.max_message)
                request_id = packet.get("id")
                if request_id is not None and request_id in self._pending:
                    self._pending.pop(request_id).set_result(packet)
                elif "event" in packet:
                    await self._events.put(packet)
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError) as exc:
            for future in self._pending.values():
                if not future.done():
                    future.set_exception(ConnectionError("daemon connection closed"))
            self._pending.clear()
            if isinstance(exc, asyncio.CancelledError):
                raise
