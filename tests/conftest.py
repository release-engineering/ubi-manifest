import base64
import json

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


@pytest.fixture
def auth_header():
    def _auth_header(roles: list[str] = []):
        raw_context = {
            "user": {
                "authenticated": True,
                "internalUsername": "fake-user",
                "roles": roles,
            }
        }

        json_context = json.dumps(raw_context).encode("utf-8")
        b64_context = base64.b64encode(json_context)

        return {"X-RhApiPlatform-CallContext": b64_context.decode("utf-8")}

    return _auth_header
