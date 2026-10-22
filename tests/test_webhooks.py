import json

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routers import webhooks


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(webhooks, "RAW_DIR", tmp_path)
    return TestClient(app)


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_webhook_is_persisted_by_topic_and_date(client, tmp_path):
    payload = {"event": "order/created", "id": 1, "total": "10.00"}

    response = client.post("/webhooks/tiendanube", json=payload)

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "topic": "order/created"}

    files = list(tmp_path.glob("order_created/*/*.json"))
    assert len(files) == 1
    record = json.loads(files[0].read_text(encoding="utf-8"))
    assert record["_topic"] == "order/created"
    assert record["payload"] == payload
    assert "_received_at" in record


def test_topic_field_is_accepted_as_alias(client):
    response = client.post("/webhooks/tiendanube", json={"topic": "product/updated", "id": 7})
    assert response.status_code == 200


def test_unknown_topic_is_rejected(client, tmp_path):
    response = client.post("/webhooks/tiendanube", json={"event": "store/deleted"})

    assert response.status_code == 422
    assert not list(tmp_path.iterdir())


def test_invalid_json_is_rejected(client):
    response = client.post(
        "/webhooks/tiendanube",
        content=b"not json",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400
