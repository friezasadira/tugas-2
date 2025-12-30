# src/nodes/queue_node.py
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as redis


def _hash_u32(s: str) -> int:
    h = hashlib.sha256(s.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big", signed=False)


@dataclass
class QueueRoute:
    owner_node_id: str
    stream: str
    group: str


class QueueNode:
    """
    Minimal distributed queue:
    - Routing: consistent-ish hashing (mod ring) over node IDs
    - Persistence: Redis Streams
    - At-least-once: consumer group + XACK (message may be redelivered if not acked)
    """

    def __init__(
        self,
        *,
        node_id: str,
        all_node_ids: List[str],
        redis_url: str,
        stream_prefix: str = "q",
        group_prefix: str = "g",
    ) -> None:
        self.node_id = node_id
        self.all_node_ids = sorted(all_node_ids)
        self.redis_url = redis_url
        self.stream_prefix = stream_prefix
        self.group_prefix = group_prefix
        self.rds: Optional[redis.Redis] = None

    async def start(self) -> None:
        self.rds = redis.from_url(self.redis_url, decode_responses=True)

    async def stop(self) -> None:
        if self.rds is not None:
            await self.rds.close()

    def route(self, queue_key: str) -> QueueRoute:
        """
        Deterministic mapping queue_key -> owner node.
        (Untuk demo consistent hashing: stabil selama list node IDs sama.)
        """
        idx = _hash_u32(queue_key) % len(self.all_node_ids)
        owner = self.all_node_ids[idx]
        stream = f"{self.stream_prefix}:{queue_key}"
        group = f"{self.group_prefix}:{queue_key}"
        return QueueRoute(owner_node_id=owner, stream=stream, group=group)

    async def enqueue(self, queue_key: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        assert self.rds is not None
        r = self.route(queue_key)
        msg_id = await self.rds.xadd(r.stream, payload)
        return {"ok": True, "queue_key": queue_key, "stream": r.stream, "id": msg_id, "owner": r.owner_node_id}

    async def ensure_group(self, stream: str, group: str) -> None:
        """
        Create consumer group if not exists.
        MKSTREAM ensures stream created.
        """
        assert self.rds is not None
        try:
            await self.rds.xgroup_create(stream, group, id="0-0", mkstream=True)
        except Exception as e:
            # BUSYGROUP = already exists
            if "BUSYGROUP" not in str(e):
                raise

    async def consume_one(
        self,
        *,
        queue_key: str,
        consumer: str,
        block_ms: int = 1000,
        count: int = 1,
    ) -> Dict[str, Any]:
        """
        Read 1 message using consumer group.
        Returns:
          - message if available
          - ok:true, message:null if none
        """
        assert self.rds is not None
        r = self.route(queue_key)
        await self.ensure_group(r.stream, r.group)

        # '>' means new messages never delivered to this group
        resp = await self.rds.xreadgroup(
            groupname=r.group,
            consumername=consumer,
            streams={r.stream: ">"},
            count=count,
            block=block_ms,
        )
        if not resp:
            return {"ok": True, "queue_key": queue_key, "message": None, "owner": r.owner_node_id}

        # resp format: [(stream, [(id, {field:value})])]]
        _, items = resp[0]
        msg_id, fields = items[0]
        return {
            "ok": True,
            "queue_key": queue_key,
            "owner": r.owner_node_id,
            "stream": r.stream,
            "group": r.group,
            "id": msg_id,
            "fields": fields,
        }

    async def ack(self, queue_key: str, msg_id: str) -> Dict[str, Any]:
        assert self.rds is not None
        r = self.route(queue_key)
        await self.ensure_group(r.stream, r.group)
        n = await self.rds.xack(r.stream, r.group, msg_id)
        return {"ok": True, "queue_key": queue_key, "id": msg_id, "acked": int(n)}

    async def pending_summary(self, queue_key: str) -> Dict[str, Any]:
        """
        Quick evidence for at-least-once: show XPENDING summary.
        """
        assert self.rds is not None
        r = self.route(queue_key)
        await self.ensure_group(r.stream, r.group)
        # XPENDING key group -> summary
        summary = await self.rds.xpending(r.stream, r.group)
        # redis-py returns dict-like / tuple depending version; keep raw for demo
        return {"ok": True, "queue_key": queue_key, "stream": r.stream, "group": r.group, "pending": summary}
