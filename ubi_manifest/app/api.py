import json
from datetime import datetime, timedelta

import redis
import requests
from fastapi import APIRouter, HTTPException

from ubi_manifest import auth
from ubi_manifest.worker.tasks.celery import app
from ubi_manifest.worker.tasks.depsolve import depsolve_task

from .models import (
    DepsolveItem,
    DepsolverResult,
    DepsolverResultItem,
    StatusResult,
    TaskState,
)
from .utils import get_gitlab_healthcheck_url, get_items_for_depsolving

REQUEST_TIMEOUT = 20

router = APIRouter(prefix="/api/v1")


@router.get(
    "/status",
    response_model=StatusResult,
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "server_status": "OK",
                        "workers_status": {
                            "availability": {"worker01": {"ok": "pong"}},
                            "stats": {"worker01": {"some": "stats"}},
                            "registered_tasks": {"worker01": ["some_task"]},
                            "active_tasks": {"worker01": []},
                            "scheduled_tasks": {"worker01": []},
                        },
                        "redis_status": {"status": "OK", "msg": "Redis is available."},
                        "celery_beat_status": {
                            "status": "OK",
                            "msg": "Celery beat operable.",
                        },
                        "connection_to_gitlab": {
                            "status": "OK",
                            "msg": "Gitlab available.",
                        },
                        "connection_to_pulp": {
                            "status": "OK",
                            "msg": "Pulp available.",
                        },
                    }
                }
            },
        }
    },
)
def status() -> StatusResult:
    # Check workers
    # All calls return None if no workers are available - no exception handling needed
    i = app.control.inspect()
    workers_status = {
        "availability": i.ping(),
        "stats": i.stats(),
        "registered_tasks": i.registered(),
        "active_tasks": i.active(),
        "scheduled_tasks": i.scheduled(),
    }

    # Check redis
    redis_client = redis.from_url(app.conf.result_backend)
    try:
        redis_client.ping()
        redis_status = {"status": "OK", "msg": "Redis is available."}
    except Exception as ex:  # pylint: disable=broad-except
        redis_status = {"status": "Failed", "msg": str(ex)}

    # Check celery beat
    heartbeat = redis_client.get("celery-beat-heartbeat")
    if heartbeat:
        last_heartbeat = datetime.fromisoformat(heartbeat.decode("utf-8"))
        now = datetime.now()
        time_from_last_heartbeat = now - last_heartbeat
        if time_from_last_heartbeat < timedelta(minutes=2):
            celery_beat_status = {"status": "OK", "msg": "Celery beat operable."}
        else:
            days = time_from_last_heartbeat.days
            hours = time_from_last_heartbeat.seconds // 3600
            minutes = (time_from_last_heartbeat.seconds - hours * 3600) // 60
            celery_beat_status = {
                "status": "Failed",
                "msg": f"Last heartbeat task ran {days} days, {hours} hours "
                f"and {minutes} minutes ago.",
            }
    else:
        celery_beat_status = {
            "status": "n/a",
            "msg": "No heartbeat task ran yet. Wait a minute.",
        }

    # GitLab is used only if the CDN definitions or content configs are loaded
    # from it. In case both the definitions and the configs are loaded from directory,
    # connection to GitLab is not needed, therefore not checked.
    gitlab_hc_url = get_gitlab_healthcheck_url()
    if gitlab_hc_url:
        try:
            gitlab_resp = requests.get(gitlab_hc_url, timeout=REQUEST_TIMEOUT)
            gitlab_resp.raise_for_status()
            gitlab_status = {"status": gitlab_resp.reason, "msg": "Gitlab available."}
        except Exception as ex:  # pylint: disable=broad-except
            gitlab_status = {"status": "Failed", "msg": str(ex)}
    else:
        gitlab_status = {"status": "n/a", "msg": "Gitlab is not needed."}

    # Check connection to Pulp
    try:
        pulp_resp = requests.get(
            f"{app.conf.pulp_url}/pulp/api/v2/status", timeout=REQUEST_TIMEOUT
        )
        pulp_status = {"status": pulp_resp.reason, "msg": "Pulp available."}
        pulp_resp.raise_for_status()
    except Exception as ex:  # pylint: disable=broad-except
        pulp_status = {"status": "Failed", "msg": str(ex)}

    status_result = StatusResult(
        server_status="OK",
        workers_status=workers_status,
        redis_status=redis_status,
        celery_beat_status=celery_beat_status,
        connection_to_gitlab=gitlab_status,
        connection_to_pulp=pulp_status,
    )
    return status_result


@router.post(
    "/manifest",
    response_model=list[TaskState],
    status_code=201,
    dependencies=[auth.needs_role("creator")],
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

    depsolve_items = get_items_for_depsolving(app.conf, depsolve_item.repo_ids)
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
    dependencies=[auth.needs_role("reader")],
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
    dependencies=[auth.needs_role("reader")],
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
