import pytest
from fastapi.testclient import TestClient
from pubtools.pulplib import FakeController

from ubi_manifest.app.factory import create_app


@pytest.fixture
def client():
    app = create_app()
    yield TestClient(app)


@pytest.fixture(name="pulp")
def fake_pulp():
    yield FakeController()
