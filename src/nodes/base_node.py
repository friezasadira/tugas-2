from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

import redis.asyncio as redis

from src.consensus.raft import RaftNode

log = logging.getLogger(__name__)


@dataclass
class BaseNode:
    node_id: str
    peers: List[str]
    redis_url: str
    self_url: str

    redis_client: Optional[redis.Redis] = None
    raft: Optional[RaftNode] = None

    async def start(self) -> None:
        log.info("BaseNode.start begin node=%s self_url=%s peers=%s", self.node_id, self.self_url, self.peers)

        self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
        await self.redis_client.ping()
        log.info("BaseNode.start after redis ping node=%s", self.node_id)

        raft_peers = [p for p in self.peers if p != self.self_url]
        log.info("BaseNode.start raft peers node=%s peers=%s", self.node_id, raft_peers)

        self.raft = RaftNode(
            node_id=self.node_id,
            peers=raft_peers,
            self_url=self.self_url,
        )

        log.info("BaseNode.start starting raft node=%s", self.node_id)
        await self.raft.start()
        log.info("BaseNode.start raft started node=%s", self.node_id)

    async def stop(self) -> None:
        if self.raft is not None:
            await self.raft.stop()
            self.raft = None
        if self.redis_client is not None:
            await self.redis_client.aclose()
            self.redis_client = None
