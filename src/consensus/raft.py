from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from src.consensus.state_machine import InMemoryStateMachine
from src.nodes.lock_manager import DistributedLockStateMachine
from src.utils.config import settings

log = logging.getLogger(__name__)


class RaftRole(str):
    follower = "follower"
    candidate = "candidate"
    leader = "leader"


@dataclass
class LogEntry:
    term: int
    cmd: Dict[str, Any]


@dataclass
class RaftPersistentState:
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)


@dataclass
class RaftVolatileState:
    commit_index: int = -1
    last_applied: int = -1


def _atomic_write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


@dataclass
class RaftNode:
    node_id: str
    peers: List[str]
    self_url: str
    election_timeout_min_ms: int = 800
    election_timeout_max_ms: int = 1500
    heartbeat_interval_ms: int = 250

    role: str = RaftRole.follower
    leader_id: Optional[str] = None

    ps: RaftPersistentState = field(default_factory=RaftPersistentState)
    vs: RaftVolatileState = field(default_factory=RaftVolatileState)

    sm: InMemoryStateMachine = field(default_factory=DistributedLockStateMachine)

    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)

    _session: Optional[aiohttp.ClientSession] = None
    _stopped: asyncio.Event = field(default_factory=asyncio.Event)
    _tasks: List[asyncio.Task] = field(default_factory=list)

    _last_contact: float = field(default_factory=lambda: time.monotonic())

    # monotonic seq for lock_acquire (victim selection)
    _cmd_seq: int = 0

    # persistence
    _state_path: Optional[Path] = None

    def _init_paths(self) -> None:
        data_dir = getattr(settings, "raft_data_dir", "/data")
        self._state_path = Path(data_dir) / "raft_state.json"

    def _persist(self) -> None:
        if self._state_path is None:
            return
        data = {
            "current_term": self.ps.current_term,
            "voted_for": self.ps.voted_for,
            "log": [{"term": e.term, "cmd": e.cmd} for e in self.ps.log],
            "_cmd_seq": self._cmd_seq,
        }
        _atomic_write_json(self._state_path, data)

    def _load_persisted(self) -> None:
        if self._state_path is None or not self._state_path.exists():
            return
        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
            self.ps.current_term = int(raw.get("current_term", 0))
            self.ps.voted_for = raw.get("voted_for", None)
            loaded_log = raw.get("log", []) or []
            self.ps.log = [LogEntry(term=int(e["term"]), cmd=dict(e["cmd"])) for e in loaded_log]
            self._cmd_seq = int(raw.get("_cmd_seq", 0))
            log.info(
                "raft.persistence_loaded node=%s path=%s log_len=%s term=%s",
                self.node_id,
                str(self._state_path),
                len(self.ps.log),
                self.ps.current_term,
            )
        except Exception as e:
            log.error("raft.persistence_load_failed node=%s path=%s err=%r", self.node_id, str(self._state_path), e)

    def last_log_index(self) -> int:
        return len(self.ps.log) - 1

    def last_log_term(self) -> int:
        if not self.ps.log:
            return 0
        return self.ps.log[-1].term

    def _new_timeout(self) -> float:
        return random.uniform(self.election_timeout_min_ms, self.election_timeout_max_ms) / 1000.0

    def _touch(self) -> None:
        self._last_contact = time.monotonic()

    async def start(self) -> None:
        random.seed(f"{self.node_id}-{time.time()}")

        self._init_paths()
        self._load_persisted()

        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2.5))

        self._stopped.clear()
        self._touch()

        log.info("raft.start node=%s peers=%s", self.node_id, self.peers)

        self._tasks = [
            asyncio.create_task(self._election_loop(), name=f"raft-election-{self.node_id}"),
            asyncio.create_task(self._heartbeat_loop(), name=f"raft-heartbeat-{self.node_id}"),
            asyncio.create_task(self._apply_loop(), name=f"raft-apply-{self.node_id}"),
        ]

        def _log_task_result(t: asyncio.Task) -> None:
            try:
                exc = t.exception()
                if exc is not None:
                    log.error("raft.task_crashed node=%s task=%s err=%r", self.node_id, t.get_name(), exc)
            except asyncio.CancelledError:
                pass

        for t in self._tasks:
            t.add_done_callback(_log_task_result)

    async def stop(self) -> None:
        self._stopped.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ---------------- client API ----------------

    async def submit_command(self, cmd: Dict[str, Any]) -> Dict[str, Any]:
        if self.role != RaftRole.leader:
            return {"ok": False, "error": "not_leader", "leader_id": self.leader_id}

        # inject deterministic ordering for deadlock victim selection
        if cmd.get("type") == "lock_acquire" and "seq" not in cmd:
            self._cmd_seq += 1
            cmd = dict(cmd)
            cmd["seq"] = self._cmd_seq

        entry = LogEntry(term=self.ps.current_term, cmd=cmd)
        self.ps.log.append(entry)
        self._persist()

        index = self.last_log_index()

        log.info(
            "raft.command_append node=%s term=%s index=%s cmd_type=%s",
            self.node_id,
            self.ps.current_term,
            index,
            cmd.get("type"),
        )

        majority = (len(self.peers) + 1) // 2 + 1

        async def replicate_once() -> int:
            async def replicate(peer: str) -> bool:
                return await self._replicate_to_peer(peer)

            ok = 0
            results = await asyncio.gather(*(replicate(p) for p in self.peers), return_exceptions=True)
            for r in results:
                if r is True:
                    ok += 1
            return ok

        acks = 1
        for _ in range(8):
            ok = await replicate_once()
            acks = 1 + ok
            if acks >= majority:
                break
            await asyncio.sleep(0.05)

        if acks >= majority:
            self.vs.commit_index = max(self.vs.commit_index, index)
            log.info("raft.command_committed node=%s index=%s acks=%s", self.node_id, index, acks)
            return {"ok": True, "index": index, "term": self.ps.current_term, "acks": acks}

        log.warning("raft.command_not_committed node=%s index=%s acks=%s", self.node_id, index, acks)
        return {"ok": False, "error": "not_committed_yet", "index": index, "acks": acks}

    # ---------------- RPC handlers ----------------

    async def on_request_vote(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        term = int(payload["term"])
        candidate_id = str(payload["candidate_id"])
        last_log_index = int(payload.get("last_log_index", -1))
        last_log_term = int(payload.get("last_log_term", 0))

        if term < self.ps.current_term:
            return {"term": self.ps.current_term, "vote_granted": False}

        if term > self.ps.current_term:
            self.ps.current_term = term
            self.ps.voted_for = None
            self.role = RaftRole.follower
            self.leader_id = None
            self._persist()

        my_last_term = self.last_log_term()
        my_last_idx = self.last_log_index()
        up_to_date = (last_log_term > my_last_term) or (last_log_term == my_last_term and last_log_index >= my_last_idx)

        vote_granted = False
        if up_to_date and (self.ps.voted_for is None or self.ps.voted_for == candidate_id):
            self.ps.voted_for = candidate_id
            vote_granted = True
            self._touch()
            self._persist()

        return {"term": self.ps.current_term, "vote_granted": vote_granted}

    async def on_append_entries(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        term = int(payload["term"])
        leader_id = str(payload["leader_id"])
        prev_log_index = int(payload.get("prev_log_index", -1))
        prev_log_term = int(payload.get("prev_log_term", 0))
        entries = payload.get("entries", []) or []
        leader_commit = int(payload.get("leader_commit", -1))

        if term < self.ps.current_term:
            return {"term": self.ps.current_term, "success": False}

        if term > self.ps.current_term:
            self.ps.current_term = term
            self.ps.voted_for = None
            self._persist()

        self.role = RaftRole.follower
        self.leader_id = leader_id
        self._touch()

        if prev_log_index >= 0:
            if prev_log_index >= len(self.ps.log):
                return {"term": self.ps.current_term, "success": False}
            if self.ps.log[prev_log_index].term != prev_log_term:
                self.ps.log = self.ps.log[:prev_log_index]
                self._persist()
                return {"term": self.ps.current_term, "success": False}

        if entries:
            insert_at = prev_log_index + 1
            if insert_at < len(self.ps.log):
                self.ps.log = self.ps.log[:insert_at]
            for e in entries:
                self.ps.log.append(LogEntry(term=int(e["term"]), cmd=dict(e["cmd"])))
            self._persist()

        if leader_commit > self.vs.commit_index:
            self.vs.commit_index = min(leader_commit, self.last_log_index())

        return {"term": self.ps.current_term, "success": True}

    # ---------------- internal loops ----------------

    async def _election_loop(self) -> None:
        timeout = self._new_timeout()
        last_tick_log = time.monotonic()

        while not self._stopped.is_set():
            await asyncio.sleep(0.05)

            now = time.monotonic()
            if now - last_tick_log >= 2.0:
                last_tick_log = now
                log.info(
                    "raft.election_tick node=%s role=%s term=%s leader=%s",
                    self.node_id,
                    self.role,
                    self.ps.current_term,
                    self.leader_id,
                )

            if self.role == RaftRole.leader:
                continue

            elapsed = time.monotonic() - self._last_contact
            if elapsed >= timeout:
                await self._start_election()
                timeout = self._new_timeout()
                self._touch()

    async def _start_election(self) -> None:
        self.role = RaftRole.candidate
        self.ps.current_term += 1
        self.ps.voted_for = self.node_id
        self.leader_id = None
        self._persist()

        term = self.ps.current_term
        votes = 1
        majority = (len(self.peers) + 1) // 2 + 1

        log.info("raft.election_start node=%s term=%s peers=%s majority=%s", self.node_id, term, self.peers, majority)

        req = {
            "term": term,
            "candidate_id": self.node_id,
            "last_log_index": self.last_log_index(),
            "last_log_term": self.last_log_term(),
        }

        async def ask(peer: str) -> Tuple[str, Optional[Dict[str, Any]]]:
            try:
                assert self._session is not None
                async with self._session.post(f"{peer}/raft/request_vote", json=req) as resp:
                    return peer, await resp.json()
            except Exception:
                return peer, None

        results = await asyncio.gather(*(ask(p) for p in self.peers), return_exceptions=False)

        for peer, r in results:
            if self.role != RaftRole.candidate:
                return
            if not r:
                log.warning("raft.vote_no_response node=%s peer=%s", self.node_id, peer)
                continue

            r_term = int(r.get("term", 0))
            if r_term > self.ps.current_term:
                self.ps.current_term = r_term
                self.role = RaftRole.follower
                self.ps.voted_for = None
                self.leader_id = None
                self._touch()
                self._persist()
                return

            if bool(r.get("vote_granted", False)) and term == self.ps.current_term:
                votes += 1

        log.info("raft.election_result node=%s term=%s votes=%s", self.node_id, term, votes)

        if self.role == RaftRole.candidate and votes >= majority:
            self.role = RaftRole.leader
            self.leader_id = self.node_id

            last = self.last_log_index() + 1
            self.next_index = {p: last for p in self.peers}
            self.match_index = {p: -1 for p in self.peers}

            log.info("raft.leader_elected node=%s term=%s", self.node_id, self.ps.current_term)
            self._touch()

    async def _heartbeat_loop(self) -> None:
        while not self._stopped.is_set():
            await asyncio.sleep(self.heartbeat_interval_ms / 1000.0)
            if self.role != RaftRole.leader:
                continue
            await self._send_heartbeats()

    async def _send_heartbeats(self) -> None:
        async def send(peer: str) -> None:
            await self._replicate_to_peer(peer, heartbeat_only=True)

        await asyncio.gather(*(send(p) for p in self.peers), return_exceptions=True)

    async def _replicate_to_peer(self, peer: str, heartbeat_only: bool = False) -> bool:
        if self.role != RaftRole.leader:
            return False

        assert self._session is not None
        ni = self.next_index.get(peer, self.last_log_index() + 1)

        prev_index = ni - 1
        prev_term = 0
        if prev_index >= 0 and prev_index < len(self.ps.log):
            prev_term = self.ps.log[prev_index].term

        entries: List[Dict[str, Any]] = []
        if not heartbeat_only:
            for e in self.ps.log[ni:]:
                entries.append({"term": e.term, "cmd": e.cmd})

        payload = {
            "term": self.ps.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_index,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": self.vs.commit_index,
            "ts": time.time(),
        }

        try:
            async with self._session.post(f"{peer}/raft/append_entries", json=payload) as resp:
                r = await resp.json()
        except Exception:
            return False

        r_term = int(r.get("term", 0))
        if r_term > self.ps.current_term:
            self.ps.current_term = r_term
            self.role = RaftRole.follower
            self.ps.voted_for = None
            self.leader_id = None
            self._touch()
            self._persist()
            return False

        success = bool(r.get("success", False))
        if success:
            if entries:
                self.match_index[peer] = self.last_log_index()
                self.next_index[peer] = self.last_log_index() + 1
                return True
            return True

        self.next_index[peer] = max(0, ni - 1)
        return False

    async def _apply_loop(self) -> None:
        while not self._stopped.is_set():
            await asyncio.sleep(0.05)
            while self.vs.last_applied < self.vs.commit_index:
                self.vs.last_applied += 1
                entry = self.ps.log[self.vs.last_applied]
                self.sm.apply(entry.cmd)
                log.info(
                    "raft.applied node=%s index=%s cmd_type=%s",
                    self.node_id,
                    self.vs.last_applied,
                    entry.cmd.get("type"),
                )

    def status(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "self_url": self.self_url,
            "role": self.role,
            "term": self.ps.current_term,
            "leader_id": self.leader_id,
            "log_len": len(self.ps.log),
            "commit_index": self.vs.commit_index,
            "last_applied": self.vs.last_applied,
            "sm": self.sm.snapshot(),
        }
