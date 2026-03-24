from fastapi.testclient import TestClient

from app.server import app


def test_health():
    c = TestClient(app)
    r = c.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_fleet_assets():
    c = TestClient(app)
    r = c.get("/api/fleet/assets?industry=mining")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)


def test_agent_chat():
    c = TestClient(app)
    r = c.post("/api/agent/chat", json={"messages": [{"role": "user", "content": "help"}]})
    assert r.status_code == 200
    assert "choices" in r.json()
