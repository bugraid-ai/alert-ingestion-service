from fastapi.testclient import TestClient
from fastapi_alert_ingestion import app

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200

    data = response.json()
    # Check required fields
    assert "status" in data
    assert "timestamp" in data
    assert "version" in data
    # Check expected value
    assert data["status"] == "healthy"
