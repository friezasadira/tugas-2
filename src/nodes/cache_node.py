# src/nodes/cache_node.py
from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import redis.asyncio as redis


@dataclass
class CacheLine:
    value: Any
    state: str  # "M", "E", "S", "I"
    ts: float


class CacheNode:
    """
    Minimal MESI-style cache (demo-grade):
    - Local cache lines with MESI states (M/E/S/I)
    - Invalidation broadcast on write if other caches might have copies
    - LRU eviction using OrderedDict
    - Metrics: hit/miss/evict/invalidate counts
    - Backing store in Redis (acts as 'memory')
    """

    def __init__(
        self,
        *,
        node_id: str,
        peers: List[str],          # list of peer base URLs, e.g. ["http://node2:8000", ...]
        redis_url: str,
        capacity: int = 100,
        redis_prefix: str = "kv",
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self.node_id = node_id
        self.peers = [p for p in peers if p and node_id not in p]  # rough filter
        self.redis_url = redis_url
        self.capacity = capacity
        self.redis_prefix = redis_prefix

        self._cache: "OrderedDict[str, CacheLine]" = OrderedDict()
        self.metrics: Dict[str, int] = {
            "hit": 0,
            "miss": 0,
            "evict": 0,
            "invalidate_sent": 0,
            "invalidate_recv": 0,
        }

        self.rds: Optional[redis.Redis] = None
        self._owned_session = False
        self.session = session

    async def start(self) -> None:
        self.rds = redis.from_url(self.redis_url, decode_responses=True)
        if self.session is None:
            self.session = aiohttp.ClientSession()
            self._owned_session = True

    async def stop(self) -> None:
        if self.rds is not None:
            await self.rds.close()
        if self.session is not None and self._owned_session:
            await self.session.close()

    def _evict_if_needed(self) -> None:
        while len(self._cache) > self.capacity:
            _, _line = self._cache.popitem(last=False)  # LRU: pop oldest
            self.metrics["evict"] += 1

    def _touch(self, key: str) -> None:
        # move to end (most recently used)
        if key in self._cache:
            self._cache.move_to_end(key, last=True)

    def _redis_key(self, key: str) -> str:
        return f"{self.redis_prefix}:{key}"

    async def _read_memory(self, key: str) -> Optional[str]:
        assert self.rds is not None
        return await self.rds.get(self._redis_key(key))

    async def _write_memory(self, key: str, value: str) -> None:
        assert self.rds is not None
        await self.rds.set(self._redis_key(key), value)

    async def get(self, key: str) -> Dict[str, Any]:
        # Cache hit (valid states M/E/S)
        if key in self._cache and self._cache[key].state != "I":
            self.metrics["hit"] += 1
            self._touch(key)
            line = self._cache[key]
            return {"ok": True, "key": key, "value": line.value, "state": line.state, "source": "cache"}

        # Cache miss / Invalid
        self.metrics["miss"] += 1
        val = await self._read_memory(key)

        # If memory empty => return null (still cache I)
        if val is None:
            return {"ok": True, "key": key, "value": None, "state": "I", "source": "memory"}

        # For demo: first fetch becomes E (exclusive) optimistically
        self._cache[key] = CacheLine(value=val, state="E", ts=time.time())
        self._touch(key)
        self._evict_if_needed()
        return {"ok": True, "key": key, "value": val, "state": "E", "source": "memory"}

    async def put(self, key: str, value: str) -> Dict[str, Any]:
        # In MESI, write requires M/E. If S, must invalidate others first. [web:10]
        # For demo: always broadcast invalidation before write to keep it simple.
        await self.invalidate_others(key)

        # Write to memory (write-through for simplicity)
        await self._write_memory(key, value)

        # Update local line as Modified
        self._cache[key] = CacheLine(value=value, state="M", ts=time.time())
        self._touch(key)
        self._evict_if_needed()
        return {"ok": True, "key": key, "value": value, "state": "M"}

    async def invalidate_others(self, key: str) -> None:
        if not self.peers:
            return
        assert self.session is not None
        for p in self.peers:
            try:
                self.metrics["invalidate_sent"] += 1
                await self.session.post(f"{p}/cache/invalidate", json={"key": key, "from": self.node_id})
            except Exception:
                # demo-grade: ignore failures
                pass

    async def on_invalidate(self, key: str, _from: str) -> Dict[str, Any]:
        self.metrics["invalidate_recv"] += 1
        if key in self._cache:
            line = self._cache[key]
            line.state = "I"
            line.ts = time.time()
            self._cache[key] = line
            self._touch(key)
        return {"ok": True, "key": key, "state": self._cache.get(key, CacheLine(None, 'I', time.time())).state}

    def state(self) -> Dict[str, Any]:
        snap = {k: {"state": v.state, "value": v.value} for k, v in self._cache.items()}
        return {"ok": True, "node_id": self.node_id, "size": len(self._cache), "capacity": self.capacity, "metrics": self.metrics, "cache": snap}
