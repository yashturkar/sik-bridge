"""Pending-send and duplicate-cache primitives."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class PendingSend:
    frame: bytes
    deadline: float
    retries_left: int
    request_context: Any = None


class DuplicateCache:
    def __init__(self, ttl: float, max_size: int) -> None:
        self.ttl = ttl
        self.max_size = max_size
        self._seen: OrderedDict[int, float] = OrderedDict()

    def contains_or_add(self, seq: int, now: float) -> bool:
        self.prune(now)
        duplicate = seq in self._seen
        self._seen[seq] = now
        self._seen.move_to_end(seq)
        while len(self._seen) > self.max_size:
            self._seen.popitem(last=False)
        return duplicate

    def prune(self, now: float) -> None:
        while self._seen:
            _, seen_at = next(iter(self._seen.items()))
            if now - seen_at <= self.ttl:
                break
            self._seen.popitem(last=False)
