import time
import pytest
import requests

PORTS = [8001, 8002, 8003]

def leader_port():
    for p in PORTS:
        try:
            st = requests.get(f"http://localhost:{p}/raft/status", timeout=2).json()
            if st.get("role") == "leader":
                return p
        except Exception:
            pass
    return None

@pytest.mark.performance
def test_50_acquires_time():
    leader = leader_port()
    assert leader is not None, "No leader found; is docker compose up?"

    t0 = time.time()
    for i in range(50):
        r = requests.post(
            f"http://localhost:{leader}/locks/acquire",
            json={"resource": f"P{i}", "mode": "X", "client_id": f"perf{i}"},
            timeout=3,
        )
        assert r.status_code == 200
    elapsed = time.time() - t0
    # threshold longgar aja, yang penting ada test perf
    assert elapsed < 30
