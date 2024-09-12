from typing import Any

from pydantic import BaseModel  # pylint: disable=no-name-in-module


class DepsolveItem(BaseModel):
    repo_ids: list[str]


class TaskState(BaseModel):
    task_id: str
    state: str


class DepsolverResultItem(BaseModel):
    src_repo_id: str
    unit_type: str
    unit_attr: str
    value: str


class DepsolverResult(BaseModel):
    repo_id: str
    content: list[DepsolverResultItem]


class StatusResult(BaseModel):
    server_status: str
    workers_status: dict[str, Any]
    redis_status: dict[str, str]
    celery_beat_status: dict[str, str]
    connection_to_gitlab: dict[str, str]
    connection_to_pulp: dict[str, str]
