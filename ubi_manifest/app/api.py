import json

import redis
from fastapi import APIRouter, HTTPException

from ubi_manifest.worker.tasks.celery import app
from ubi_manifest.worker.tasks.depsolve import depsolve_task

from .models import DepsolveItem, DepsolverResult, DepsolverResultItem, TaskState
from .utils import get_items_for_depsolving, get_repo_classes

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
    if not depsolve_item.repo_ids:
        raise HTTPException(
            status_code=400,
            detail="No repo IDs were provided.",
        )

    repo_classes = get_repo_classes(app.conf.content_config, depsolve_item.repo_ids)
    # we expect exactly one repo class in one request
    if len(repo_classes) > 1:
        raise HTTPException(
            status_code=400,
            detail=f"Can't process repos from different classes {repo_classes} "
            "in one request. Please make separate request for each class.",
        )
    if len(repo_classes) == 0:
        raise HTTPException(
            status_code=404,
            detail=f"Given repos {depsolve_item.repo_ids} have unexpected ids. "
            "It seems they are not from any of the accepted repo classes "
            f"{list(app.conf.content_config.keys())} defined in content config.",
        )

    depsolve_items = get_items_for_depsolving(
        app.conf, depsolve_item.repo_ids, repo_classes[0]
    )
    if not depsolve_items:
        raise HTTPException(
            status_code=404,
            detail=f"No depsolve items were identified for {depsolve_item.repo_ids}.",
        )

    tasks_states = []
    for item in depsolve_items:
        task = depsolve_task.apply_async(args=[item["repo_group"], item["url"]])
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
