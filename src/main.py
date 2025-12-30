import logging
from aiohttp import web

from src.utils.config import settings
from src.nodes.base_node import BaseNode
from src.nodes.queue_node import QueueNode
from src.nodes.cache_node import CacheNode

log = logging.getLogger(__name__)


def _peer_id(p) -> str:
    if isinstance(p, dict):
        return p.get("id") or p.get("node_id") or p.get("name") or ""
    if isinstance(p, str):
        s = p.replace("http://", "").replace("https://", "")
        host = s.split("/")[0]
        return host.split(":")[0]
    return ""


def _peer_url(p) -> str:
    if isinstance(p, dict):
        return p.get("url") or p.get("self_url") or p.get("addr") or ""
    if isinstance(p, str):
        return p
    return ""


async def create_app() -> web.Application:
    app = web.Application()

    self_url = f"http://{settings.node_id}:{settings.http_port}"
    peers_raw = settings.peer_list()

    node = BaseNode(
        node_id=settings.node_id,
        peers=peers_raw,
        redis_url=settings.redis_url,
        self_url=self_url,
    )

    # Queue: routing uses node IDs
    all_ids = [settings.node_id] + [pid for pid in (_peer_id(p) for p in peers_raw) if pid]
    queue = QueueNode(node_id=settings.node_id, all_node_ids=all_ids, redis_url=settings.redis_url)

    # Cache: peers use URLs
    peer_urls = [u for u in (_peer_url(p) for p in peers_raw) if u and u != self_url]
    # (Optional) cache capacity via env var if ada, kalau tidak default 100
    cache_capacity = int(getattr(settings, "cache_capacity", 100))
    cache = CacheNode(node_id=settings.node_id, peers=peer_urls, redis_url=settings.redis_url, capacity=cache_capacity)

    log.info("APP: starting node.start() node_id=%s self_url=%s", settings.node_id, self_url)
    await node.start()
    await queue.start()
    await cache.start()
    log.info("APP: node.start() done node_id=%s", settings.node_id)

    def not_leader_resp() -> web.Response:
        assert node.raft is not None
        return web.json_response(
            {"ok": False, "error": "not_leader", "leader_id": node.raft.leader_id},
            status=409,
        )

    # -----------------------
    # Basic / raft
    # -----------------------
    async def health(_: web.Request) -> web.Response:
        return web.json_response({"ok": True, "node_id": settings.node_id})

    async def raft_status(_: web.Request) -> web.Response:
        assert node.raft is not None
        return web.json_response(node.raft.status())

    async def request_vote(request: web.Request) -> web.Response:
        assert node.raft is not None
        payload = await request.json()
        return web.json_response(await node.raft.on_request_vote(payload))

    async def append_entries(request: web.Request) -> web.Response:
        assert node.raft is not None
        payload = await request.json()
        return web.json_response(await node.raft.on_append_entries(payload))

    async def raft_command(request: web.Request) -> web.Response:
        assert node.raft is not None
        if node.raft.role != "leader":
            return not_leader_resp()
        cmd = await request.json()
        return web.json_response(await node.raft.submit_command(cmd))

    async def raft_debug(_: web.Request) -> web.Response:
        assert node.raft is not None
        import asyncio

        tasks = []
        for t in asyncio.all_tasks():
            tasks.append({"name": t.get_name(), "done": t.done(), "cancelled": t.cancelled()})
        return web.json_response({"raft": node.raft.status(), "tasks": tasks})

    # -----------------------
    # Locks (leader-only)
    # -----------------------
    async def locks_state(_: web.Request) -> web.Response:
        assert node.raft is not None
        if node.raft.role != "leader":
            return not_leader_resp()
        sm = node.raft.sm
        data = {"raft": node.raft.status(), "locks": getattr(sm, "snapshot_locks", lambda: {})()}
        return web.json_response(data)

    async def locks_wfg(_: web.Request) -> web.Response:
        assert node.raft is not None
        if node.raft.role != "leader":
            return not_leader_resp()

        sm = node.raft.sm
        if not hasattr(sm, "build_wait_for_graph"):
            return web.json_response({"ok": False, "error": "state_machine_no_wfg"}, status=500)

        graph = sm.build_wait_for_graph()
        cycle = sm.find_cycle(graph) if graph else None
        graph_json = {k: sorted(list(v)) for k, v in graph.items()}
        return web.json_response({"ok": True, "raft": node.raft.status(), "wfg": graph_json, "cycle": cycle})

    async def locks_acquire(request: web.Request) -> web.Response:
        assert node.raft is not None
        if node.raft.role != "leader":
            return not_leader_resp()

        body = await request.json()
        cmd = {
            "type": "lock_acquire",
            "resource": body["resource"],
            "mode": body["mode"],
            "client_id": body["client_id"],
        }
        return web.json_response(await node.raft.submit_command(cmd))

    async def locks_release(request: web.Request) -> web.Response:
        assert node.raft is not None
        if node.raft.role != "leader":
            return not_leader_resp()

        body = await request.json()
        cmd = {
            "type": "lock_release",
            "resource": body["resource"],
            "client_id": body["client_id"],
        }
        return web.json_response(await node.raft.submit_command(cmd))

    # -----------------------
    # Queue (Redis Streams)
    # -----------------------
    async def queue_route(request: web.Request) -> web.Response:
        key = request.query.get("queue_key")
        if not key:
            return web.json_response({"ok": False, "error": "missing_queue_key"}, status=400)
        r = queue.route(key)
        return web.json_response(
            {"ok": True, "queue_key": key, "owner_node_id": r.owner_node_id, "stream": r.stream, "group": r.group}
        )

    async def queue_enqueue(request: web.Request) -> web.Response:
        body = await request.json()
        queue_key = body["queue_key"]
        payload = body.get("payload", {})
        return web.json_response(await queue.enqueue(queue_key, payload))

    async def queue_consume(request: web.Request) -> web.Response:
        body = await request.json()
        queue_key = body["queue_key"]
        consumer = body.get("consumer", settings.node_id)
        block_ms = int(body.get("block_ms", 1000))
        return web.json_response(await queue.consume_one(queue_key=queue_key, consumer=consumer, block_ms=block_ms))

    async def queue_ack(request: web.Request) -> web.Response:
        body = await request.json()
        queue_key = body["queue_key"]
        msg_id = body["id"]
        return web.json_response(await queue.ack(queue_key, msg_id))

    async def queue_pending(request: web.Request) -> web.Response:
        key = request.query.get("queue_key")
        if not key:
            return web.json_response({"ok": False, "error": "missing_queue_key"}, status=400)
        return web.json_response(await queue.pending_summary(key))

    # -----------------------
    # Cache (MESI demo + LRU + metrics)
    # -----------------------
    async def cache_get(request: web.Request) -> web.Response:
        key = request.query.get("key")
        if not key:
            return web.json_response({"ok": False, "error": "missing_key"}, status=400)
        return web.json_response(await cache.get(key))

    async def cache_put(request: web.Request) -> web.Response:
        body = await request.json()
        key = body["key"]
        value = str(body["value"])
        return web.json_response(await cache.put(key, value))

    async def cache_invalidate(request: web.Request) -> web.Response:
        body = await request.json()
        key = body["key"]
        frm = body.get("from", "unknown")
        return web.json_response(await cache.on_invalidate(key, frm))

    async def cache_state(_: web.Request) -> web.Response:
        return web.json_response(cache.state())

    # Routes
    app.router.add_get("/health", health)

    app.router.add_get("/raft/status", raft_status)
    app.router.add_post("/raft/request_vote", request_vote)
    app.router.add_post("/raft/append_entries", append_entries)
    app.router.add_post("/raft/command", raft_command)
    app.router.add_get("/raft/debug", raft_debug)

    app.router.add_get("/locks/state", locks_state)
    app.router.add_get("/locks/wfg", locks_wfg)
    app.router.add_post("/locks/acquire", locks_acquire)
    app.router.add_post("/locks/release", locks_release)

    app.router.add_get("/queue/route", queue_route)
    app.router.add_post("/queue/enqueue", queue_enqueue)
    app.router.add_post("/queue/consume", queue_consume)
    app.router.add_post("/queue/ack", queue_ack)
    app.router.add_get("/queue/pending", queue_pending)

    app.router.add_get("/cache/get", cache_get)
    app.router.add_post("/cache/put", cache_put)
    app.router.add_post("/cache/invalidate", cache_invalidate)
    app.router.add_get("/cache/state", cache_state)

    async def deadlock_ctx(app: web.Application):
        import asyncio

        async def loop():
            while True:
                await asyncio.sleep(0.5)
                if node.raft is None:
                    continue
                if node.raft.role != "leader":
                    continue

                sm = node.raft.sm
                if not hasattr(sm, "build_wait_for_graph"):
                    continue

                graph = sm.build_wait_for_graph()
                if not graph:
                    continue

                cycle = sm.find_cycle(graph)
                if not cycle:
                    continue

                victim = sm.newest_seq_in_cycle(cycle)
                if not victim:
                    continue

                await node.raft.submit_command({"type": "lock_abort_client", "client_id": victim, "reason": "deadlock"})

        task = asyncio.create_task(loop(), name=f"deadlock-scan-{settings.node_id}")
        try:
            yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    app.cleanup_ctx.append(deadlock_ctx)

    async def on_cleanup(_: web.Application):
        log.info("APP: cleanup node.stop() node_id=%s", settings.node_id)
        await cache.stop()
        await queue.stop()
        await node.stop()

    app.on_cleanup.append(on_cleanup)
    return app


def main() -> None:
    logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
    logging.getLogger("src.consensus.raft").setLevel(logging.INFO)
    web.run_app(create_app(), host=settings.http_host, port=settings.http_port)


if __name__ == "__main__":
    main()
