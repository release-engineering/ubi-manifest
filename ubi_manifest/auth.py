import base64
import logging
from typing import Any, Union

from fastapi import Depends, HTTPException, Request
from pydantic import BaseModel

_LOG = logging.getLogger(__name__)


class ClientContext(BaseModel):
    """
    Call context data relating to service accounts / machine users.
    """

    roles: list[str] = []
    authenticated: bool = False
    serviceAccountId: Union[str, None] = None


class UserContext(BaseModel):
    """
    Call context data relating to human users.
    """

    roles: list[str] = []
    authenticated: bool = False
    internalUsername: Union[str, None] = None


class CallContext(BaseModel):
    """
    Represents an authenticated (or not) context for an incoming request.

    Use the fields on this model to decide whether the current request belongs
    to an authenticated user, and if so, to determine which role(s) are held
    by the user.
    """

    client: ClientContext = ClientContext()
    user: UserContext = UserContext()


def call_context(request: Request) -> CallContext:
    """
    Returns the CallContext for the current request.
    """
    header = "X-RhApiPlatform-CallContext"
    header_value = request.headers.get(header)
    if not header_value:
        _LOG.debug("No security header %s found in request", header)
        return CallContext()

    try:
        decoded = base64.b64decode(header_value, validate=True)
        return CallContext.model_validate_json(decoded)
    except Exception:
        summary = f"Invalid {header} header in request"
        _LOG.exception(summary, extra={"event": "auth", "success": False})
        raise HTTPException(400, detail=summary) from None


def caller_name(context: CallContext = Depends(call_context)) -> str:
    """
    Returns the name(s) of the calling user and/or service account.
    The returned value is appropriate only for logging.
    """

    # No idea whether it's actually possible for a request to be authenticated
    # as both a user and a serviceaccount, but the design of the call context
    # structure allows for it, so this code will tolerate it also.
    users = []
    if context.user.internalUsername:
        users.append(f"user {context.user.internalUsername}")
    if context.client.serviceAccountId:
        users.append(f"serviceaccount {context.client.serviceAccountId}")
    if not users:
        users.append("<anonymous user>")

    return " AND ".join(users)


def caller_roles(
    context: CallContext = Depends(call_context),
) -> set[str]:
    """
    Returns all roles held by the caller of the current request.
    This will be an empty set for unauthenticated requests.
    """
    return set(context.user.roles + context.client.roles)


def needs_role(role: str) -> Any:
    """
    Returns a dependency on a specific named role.

    This function is intended to be used with "dependencies" on endpoints in
    order to associate them with specific roles. Requests to that endpoint will
    fail unless the caller is authenticated as a user having that role.

    For example:

    > @app.post('/my-great-api/frobnitz', dependencies=[needs_role("xyz")])
    > def do_frobnitz():
    >    "If caller does not have role xyz, they will never get here."
    """

    def check_roles(
        request: Request,
        roles: set[str] = Depends(caller_roles),
        name: str = Depends(caller_name),
    ) -> None:
        if role not in roles:
            _LOG.warning(
                "Access denied; path=%s, user=%s, required_role=%s",
                request.url.path,
                name,
                role,
                extra={"event": "auth", "success": False},
            )
            raise HTTPException(403, f"this operation requires role '{role}'")

        _LOG.info(
            "Access permitted; path=%s, user=%s, role=%s",
            request.url.path,
            name,
            role,
            extra={"event": "auth", "success": True},
        )

    return Depends(check_roles)


def log_login(
    request: Request,
    roles: set[str] = Depends(caller_roles),
    name: str = Depends(caller_name),
) -> None:
    if name != "<anonymous user>":
        _LOG.info(
            "Login: path=%s, user=%s, roles=%s",
            request.url.path,
            name,
            roles,
            extra={"event": "login", "success": True},
        )
