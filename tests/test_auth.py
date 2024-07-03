import base64
import json
import logging
from dataclasses import dataclass
from unittest import mock

import pytest
from fastapi import HTTPException
from starlette.datastructures import URL

from ubi_manifest.auth import (
    CallContext,
    ClientContext,
    UserContext,
    call_context,
    caller_name,
    caller_roles,
    needs_role,
)


@dataclass
class FakeRequest:
    url: URL


def test_no_context():
    """Unauthenticated requests returns a default context."""

    request = mock.Mock(headers={})
    ctx = call_context(request)

    # It should return a truthy object.
    assert ctx

    # It should have no roles.
    assert not ctx.client.roles
    assert not ctx.user.roles

    # It should not be authenticated.
    assert not ctx.client.authenticated
    assert not ctx.user.authenticated


def test_decode_context():
    """A context can be decoded from a valid header."""

    raw_context = {
        "client": {
            "roles": ["someRole", "anotherRole"],
            "authenticated": True,
            "serviceAccountId": "clientappname",
        },
        "user": {
            "roles": ["reader"],
            "authenticated": True,
            "internalUsername": "greatUser",
        },
    }
    b64 = base64.b64encode(json.dumps(raw_context).encode("utf-8"))

    request = mock.Mock(headers={"X-RhApiPlatform-CallContext": b64})
    ctx = call_context(request=request)

    # The details should match exactly the encoded data from the header.
    assert ctx.client.roles == ["someRole", "anotherRole"]
    assert ctx.client.authenticated
    assert ctx.client.serviceAccountId == "clientappname"

    assert ctx.user.roles == ["reader"]
    assert ctx.user.authenticated
    assert ctx.user.internalUsername == "greatUser"


@pytest.mark.parametrize(
    "header_value",
    [
        # not valid base64
        "oops not valid",
        # valid base64, but not valid JSON
        base64.b64encode(b"oops not JSON"),
        # valid base64, valid JSON, but wrong structure
        base64.b64encode(b'["oops schema mismatch]'),
    ],
)
def test_bad_header(header_value):
    """If header does not contain valid content, a meaningful error is raised."""
    request = mock.Mock(headers={"X-RhApiPlatform-CallContext": header_value})
    with pytest.raises(HTTPException) as exc_info:
        call_context(request=request)

    # It should give a 400 error (client error)
    assert exc_info.value.status_code == 400

    # It should give some hint as to what the problem is
    assert (
        exc_info.value.detail == "Invalid X-RhApiPlatform-CallContext header in request"
    )


def test_caller_roles_empty():
    """caller_roles returns an empty set for a default (empty) context."""

    assert (caller_roles(CallContext())) == set()


def test_caller_roles_nonempty():
    """caller_roles returns all roles from the context when present."""

    ctx = CallContext(
        user=UserContext(roles=["role1", "role2"]),
        client=ClientContext(roles=["role2", "role3"]),
    )
    assert (caller_roles(ctx)) == set(["role1", "role2", "role3"])


def test_caller_name_empty():
    """caller_name returns a reasonable value for an unauthed context."""

    assert (caller_name(CallContext())) == "<anonymous user>"


def test_caller_name_simple():
    """caller_name returns a reasonable value for a typical authed context."""

    assert (
        caller_name(CallContext(user=UserContext(internalUsername="shazza")))
    ) == "user shazza"


def test_caller_name_multi():
    """caller_name returns a reasonable value for a context having both user
    and serviceaccount authentication info."""

    assert (
        caller_name(
            CallContext(
                user=UserContext(internalUsername="shazza"),
                client=ClientContext(serviceAccountId="bottle-o"),
            )
        )
    ) == "user shazza AND serviceaccount bottle-o"


def test_needs_role_success(caplog: pytest.LogCaptureFixture):
    """needs_role succeeds and logs needed role is present."""

    caplog.set_level(logging.INFO)

    fn = needs_role("better-role").dependency

    # It should succeed
    fn(
        FakeRequest(URL("/endpoint")),
        roles=set(["better-role"]),
        name="bazza",
    )

    # It should log about the successful auth
    assert (
        "Access permitted; path=/endpoint, user=bazza, role=better-role" in caplog.text
    )


def test_needs_role_fail(caplog: pytest.LogCaptureFixture):
    """needs_role logs and raises meaningful error when needed role is absent."""

    fn = needs_role("best-role").dependency

    # It should raise an exception.
    with pytest.raises(HTTPException) as exc_info:
        fn(
            FakeRequest(URL("/endpoint")),
            roles=set(["abc", "xyz"]),
            name="dazza",
        )

    # It should use status 403 to tell the client they are unauthorized.
    assert exc_info.value.status_code == 403

    # It should give some hint as to the needed role.
    assert exc_info.value.detail == "this operation requires role 'best-role'"

    # It should log about the authorization failure
    assert (
        "Access denied; path=/endpoint, user=dazza, required_role=best-role"
        in caplog.text
    )
