def test_status(client):
    response = client.get("/api/v1/status")

    assert response.status_code == 200
    assert response.json() == {"status": "OK"}
