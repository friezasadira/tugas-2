from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from src.consensus.state_machine import InMemoryStateMachine


@dataclass
class LockRequest:
    client_id: str
    mode: str  # "S" or "X"
    seq: int = 0  # for deadlock victim selection (higher = newer)


@dataclass
class LockState:
    shared_holders: Set[str] = field(default_factory=set)
    exclusive_holder: Optional[str] = None
    queue: List[LockRequest] = field(default_factory=list)


class DistributedLockStateMachine(InMemoryStateMachine):
    """
    State machine applied by Raft commit order.
    Stores locks in-memory.
    """

    def __init__(self) -> None:
        super().__init__()
        self.locks: Dict[str, LockState] = {}

    def apply(self, cmd: Dict) -> None:
        super().apply(cmd)

        t = cmd.get("type")
        if t == "lock_acquire":
            self._apply_acquire(cmd)
        elif t == "lock_release":
            self._apply_release(cmd)
        elif t == "lock_force_release":
            self._apply_force_release(cmd)
        elif t == "lock_abort_client":
            self._apply_abort_client(cmd)

    def _get(self, resource: str) -> LockState:
        if resource not in self.locks:
            self.locks[resource] = LockState()
        return self.locks[resource]

    def _can_grant(self, st: LockState, req: LockRequest) -> bool:
        if req.mode == "S":
            return st.exclusive_holder is None and (not st.queue or st.queue[0].client_id == req.client_id)
        # X
        if st.exclusive_holder is not None and st.exclusive_holder != req.client_id:
            return False
        if st.shared_holders and (st.shared_holders != {req.client_id}):
            return False
        return (not st.queue) or (st.queue[0].client_id == req.client_id)

    def _grant(self, st: LockState, req: LockRequest) -> None:
        if req.mode == "S":
            st.shared_holders.add(req.client_id)
        else:
            st.exclusive_holder = req.client_id
            st.shared_holders.discard(req.client_id)

    def _enqueue_if_missing(self, st: LockState, req: LockRequest) -> None:
        # if client already has a queued request, treat latest as authoritative
        for i, r in enumerate(st.queue):
            if r.client_id == req.client_id:
                st.queue[i] = req
                return
        st.queue.append(req)

    def _drain_queue(self, st: LockState, resource: str) -> None:
        changed = True
        while changed and st.queue:
            changed = False
            head = st.queue[0]
            if self._can_grant(st, head):
                st.queue.pop(0)
                self._grant(st, head)
                changed = True

    def _apply_acquire(self, cmd: Dict) -> None:
        resource = str(cmd["resource"])
        client_id = str(cmd["client_id"])
        mode = str(cmd["mode"]).upper()
        seq = int(cmd.get("seq", 0))

        st = self._get(resource)
        req = LockRequest(client_id=client_id, mode=mode, seq=seq)

        self._enqueue_if_missing(st, req)
        self._drain_queue(st, resource)

    def _apply_release(self, cmd: Dict) -> None:
        resource = str(cmd["resource"])
        client_id = str(cmd["client_id"])

        st = self._get(resource)

        if st.exclusive_holder == client_id:
            st.exclusive_holder = None
        st.shared_holders.discard(client_id)

        st.queue = [r for r in st.queue if r.client_id != client_id]
        self._drain_queue(st, resource)

    def _apply_force_release(self, cmd: Dict) -> None:
        resource = str(cmd["resource"])
        client_id = str(cmd["client_id"])
        st = self._get(resource)

        if st.exclusive_holder == client_id:
            st.exclusive_holder = None
        st.shared_holders.discard(client_id)
        st.queue = [r for r in st.queue if r.client_id != client_id]
        self._drain_queue(st, resource)

    def _apply_abort_client(self, cmd: Dict) -> None:
        victim = str(cmd["client_id"])

        for res, st in self.locks.items():
            changed = False

            if st.exclusive_holder == victim:
                st.exclusive_holder = None
                changed = True

            if victim in st.shared_holders:
                st.shared_holders.discard(victim)
                changed = True

            old_len = len(st.queue)
            st.queue = [r for r in st.queue if r.client_id != victim]
            if len(st.queue) != old_len:
                changed = True

            if changed:
                self._drain_queue(st, res)

    # -------- deadlock detection helpers (leader uses these) --------

    def build_wait_for_graph(self) -> Dict[str, Set[str]]:
        graph: Dict[str, Set[str]] = {}

        def add_edge(a: str, b: str) -> None:
            if a == b:
                return
            graph.setdefault(a, set()).add(b)

        for _res, st in self.locks.items():
            holders = set(st.shared_holders)
            if st.exclusive_holder:
                holders.add(st.exclusive_holder)

            if not holders or not st.queue:
                continue

            for req in st.queue:
                # if it's head AND can be granted, it is not waiting
                if st.queue and st.queue[0].client_id == req.client_id and self._can_grant(st, req):
                    continue

                if req.mode == "X":
                    for h in holders:
                        if h != req.client_id:
                            add_edge(req.client_id, h)
                else:  # S
                    if st.exclusive_holder and st.exclusive_holder != req.client_id:
                        add_edge(req.client_id, st.exclusive_holder)

        return graph

    @staticmethod
    def find_cycle(graph: Dict[str, Set[str]]) -> Optional[List[str]]:
        visited: Set[str] = set()
        stack: Set[str] = set()
        parent: Dict[str, str] = {}

        def dfs(u: str) -> Optional[List[str]]:
            visited.add(u)
            stack.add(u)

            for v in graph.get(u, set()):
                if v not in visited:
                    parent[v] = u
                    c = dfs(v)
                    if c:
                        return c
                elif v in stack:
                    # reconstruct cycle
                    cycle = [v]
                    cur = u
                    while cur != v and cur in parent:
                        cycle.append(cur)
                        cur = parent[cur]
                    cycle.append(v)
                    cycle.reverse()
                    return cycle

            stack.remove(u)
            return None

        for n in list(graph.keys()):
            if n not in visited:
                c = dfs(n)
                if c:
                    return c
        return None

    def newest_seq_in_cycle(self, cycle: List[str]) -> Optional[str]:
        cycle_set = set(cycle)
        best_client: Optional[str] = None
        best_seq: int = -1

        for _res, st in self.locks.items():
            for r in st.queue:
                if r.client_id in cycle_set and r.seq > best_seq:
                    best_seq = r.seq
                    best_client = r.client_id
        return best_client

    def snapshot_locks(self) -> Dict:
        out = {}
        for res, st in self.locks.items():
            out[res] = {
                "exclusive_holder": st.exclusive_holder,
                "shared_holders": sorted(list(st.shared_holders)),
                "queue": [{"client_id": r.client_id, "mode": r.mode, "seq": r.seq} for r in st.queue],
            }
        return out
