import time
import pytest
import requests

PORTS = [8001, 8002, 8003]

def status(port: int):
    return requests.get(f"http://localhost:{port}/raft/status", timeout=2).json()

def leader_port():
    for p in PORTS:
        try:
            st = status(p)
            if st.get("role") == "leader":
                return p
        except Exception:
            pass
    return None

@pytest.mark.integration
def test_leader_only_enforced():
    leader = leader_port()
    assert leader is not None, "No leader found; is docker compose up?"

    # pick a follower port
    follower = next(p for p in PORTS if p != leader)

    r = requests.get(f"http://localhost:{follower}/locks/state", timeout=2)
    assert r.status_code == 409

@pytest.mark.integration
def test_acquire_commit_applied():
    leader = leader_port()
    assert leader is not None, "No leader found; is docker compose up?"

    # Acquire lock
    r = requests.post(
        f"http://localhost:{leader}/locks/acquire",
        json={"resource": "T1", "mode": "X", "client_id": "test_client"},
        timeout=3,
    )
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True

    # wait small for apply loop
    time.sleep(0.2)

    st = requests.get(f"http://localhost:{leader}/raft/status", timeout=2).json()
    assert st["commit_index"] >= 0
    assert st["last_applied"] >= 0
