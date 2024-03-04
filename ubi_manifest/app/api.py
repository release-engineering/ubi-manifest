import json

import redis
from fastapi import APIRouter, HTTPException

from ubi_manifest.worker.tasks.celery import app
from ubi_manifest.worker.tasks.depsolve import depsolve_task

from .models import DepsolveItem, DepsolverResult, DepsolverResultItem, TaskState

router = APIRouter(prefix="/api/v1")


@router.get("/status")
def status() -> dict[str, str]:
    return {"status": "OK"}


@router.post(
    "/manifest",
    response_model=list[TaskState],
    status_code=201,
    responses={
        201: {
            "description": "Depsolve tasks created",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "task_id": "some_task_id",
                            "state": "PENDING",
                        }
                    ]
                }
            },
        },
        404: {
            "description": "None of repositories requested are not allowed for depsolving.",
            "content": {
                "application/json": {
                    "example": {
                        "details": "None of [repo_id] are allowed for depsolving."
                    }
                }
            },
        },
    },
)
def manifest_post(depsolve_item: DepsolveItem) -> list[TaskState]:
    repo_groups: dict[str, list[str]] = {}
    # compare provided repo_ids with the config and pick allowed repo groups
    for repo_id in depsolve_item.repo_ids:
        for key, group in app.conf.allowed_ubi_repo_groups.items():
            if repo_id in group:
                repo_groups.setdefault(key, group)

    if not repo_groups:
        raise HTTPException(
            status_code=404,
            detail=f"None of {depsolve_item.repo_ids} are allowed for depsolving.",
        )

    tasks_states = []
    for repo_group_key, repo_group in repo_groups.items():
        content_config_source = None
        for group_prefix, source in app.conf.content_config.items():
            if repo_group_key.startswith(group_prefix):
                content_config_source = source
                break

        task = depsolve_task.apply_async(args=[repo_group, content_config_source])
        tasks_states.append(TaskState(task_id=task.task_id, state=task.state))

    return tasks_states


@router.get(
    "/manifest/{repo_id}",
    response_model=DepsolverResult,
    status_code=200,
    responses={
        200: {
            "description": "Depsolved content for repo_id found",
            "content": {
                "application/json": {
                    "example": {
                        "repo_id": "foo-bar-repo",
                        "content": [
                            {
                                "src_repo_id": "source-foo-bar-repo",
                                "unit_type": "RpmUnit",
                                "unit_attr": "filename",
                                "value": "some-filename.rpm",
                            }
                        ],
                    }
                }
            },
        },
        404: {
            "description": "Content for request repository is not available.",
            "content": {
                "application/json": {
                    "example": {"detail": "Content for foo-repo not found"}
                }
            },
        },
    },
)
def manifest_get(repo_id: str) -> DepsolverResult:
    redis_client = redis.from_url(app.conf.result_backend)
    value = redis_client.get(repo_id) or ""
    if value:
        content = []
        for parsed_value in json.loads(value):
            item = DepsolverResultItem(**parsed_value)
            content.append(item)
        result = DepsolverResult(repo_id=repo_id, content=content)
        return result

    raise HTTPException(status_code=404, detail=f"Content for {repo_id} not found")


@router.get(
    "/task/{task_id}",
    response_model=TaskState,
    status_code=200,
    responses={
        200: {
            "description": "Task with task_id found",
            "content": {
                "application/json": {
                    "example": {
                        "task_id": "some-task-id",
                        "state": "PENDING",
                    }
                }
            },
        },
        404: {
            "description": "Task with task_id not found",
            "content": {
                "application/json": {
                    "example": {"detail": "Task some-other-task-id not found"}
                }
            },
        },
    },
)
def task_state(task_id: str) -> TaskState:
    task = app.AsyncResult(task_id)
    if task:
        return TaskState(task_id=task.task_id, state=task.state)

    raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
