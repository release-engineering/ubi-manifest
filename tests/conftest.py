import pytest

from ubi_manifest.app.factory import create_app
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    app = create_app()
    yield TestClient(app)
