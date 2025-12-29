import pytest

@pytest.mark.unit
def test_cycle_detection_basic():
    # graph: c1 waits c2, c2 waits c1 => cycle
    g = {"c1": {"c2"}, "c2": {"c1"}}

    def find_cycle(graph):
        visited = set()
        stack = set()

        def dfs(u):
            visited.add(u)
            stack.add(u)
            for v in graph.get(u, set()):
                if v not in visited:
                    if dfs(v):
                        return True
                elif v in stack:
                    return True
            stack.remove(u)
            return False

        for n in graph:
            if n not in visited and dfs(n):
                return True
        return False

    assert find_cycle(g) is True
