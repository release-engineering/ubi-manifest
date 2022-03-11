import json
from unittest import mock

from attr import define

from .utils import MockedRedis


@define
class MockAsyncResult:
    task_id: str
    state: str


def test_status(client):
    response = client.get("/api/v1/status")

    assert response.status_code == 200
    assert response.json() == {"status": "OK"}


def test_task_state(client):
    """test getting state of given celery task_id"""
    with mock.patch("ubi_manifest.app.api.app.AsyncResult") as task_mock:
        task_id = "some-task-id"
        task_mock.return_value = MockAsyncResult(task_id=task_id, state="PENDING")

        response = client.get(f"/api/v1/task/{task_id}")

        # 200 status code is expected
        assert response.status_code == 200
        json_data = response.json()
        # task_id and state are properly set in response
        assert json_data["task_id"] == task_id
        assert json_data["state"] == "PENDING"


def test_task_state_not_found(client):
    """test getting state of given celery task when task is not found"""
    with mock.patch("ubi_manifest.app.api.app.AsyncResult") as task_mock:
        task_id = "some-task-id"
        task_mock.return_value = None

        response = client.get(f"/api/v1/task/{task_id}")
        # 404 is expected status code
        assert response.status_code == 404
        json_data = response.json()
        # proper detail is set in the response
        assert json_data["detail"] == f"Task {task_id} not found"


def test_manifest_get(client):
    """test getting depsolved content for repository"""
    depsolver_result_item = [
        {
            "src_repo_id": "source-foo-bar-repo",
            "unit_type": "RpmUnit",
            "unit_attr": "filename",
            "value": "some-filename.rpm",
        },
    ]

    depsolver_result_item_json_str = json.dumps(depsolver_result_item)

    redis_data = {"manifest:ubi_repo_id": depsolver_result_item_json_str}

    with mock.patch("ubi_manifest.app.api.redis.from_url") as mock_redis_from_url:
        mock_redis_from_url.return_value = MockedRedis(data=redis_data)
        response = client.get(f"/api/v1/manifest/ubi_repo_id")

        # expected status code in 200
        assert response.status_code == 200
        json_data = response.json()
        # repo_id is set to the one we requested
        assert json_data["repo_id"] == "ubi_repo_id"

        content = json_data["content"]
        # there is only one unit in the content
        assert len(content) == 1
        content_item = content[0]
        # details of unit are set properly
        assert content_item["src_repo_id"] == "source-foo-bar-repo"
        assert content_item["unit_type"] == "RpmUnit"
        assert content_item["unit_attr"] == "filename"
        assert content_item["value"] == "some-filename.rpm"


def test_manifest_get_not_found(client):
    """test getting depsolved content when the cotent is not available for given repo_id"""
    with mock.patch("ubi_manifest.app.api.redis.from_url") as mock_redis_from_url:
        mock_redis_from_url.return_value = MockedRedis(data={})
        response = client.get("/api/v1/manifest/ubi_repo_id")
        # expected status code is 404
        assert response.status_code == 404
        json_data = response.json()
        # response detail is properly set
        assert json_data["detail"] == "Content for ubi_repo_id not found"


def test_manifest_post(client):
    """test request for depsolving for given repo ids"""
    with mock.patch("celery.app.task.Task.apply_async") as mocked_apply_async:
        mocked_apply_async.return_value = MockAsyncResult(
            task_id="foo-bar-id", state="PENDING"
        )

        # will request depsolving for 2 repos
        # 'repo_1' is set in the default config, and it will be depsolving
        # 'repo_not_allowed' will be skipped - not present in the config
        response = client.post(
            "/api/v1/manifest", json={"repo_ids": ["repo_1", "repo_not_allowed"]}
        )

        # depsolve task is run with 2 repos in args:
        # 'repo_1' was requested via API
        # 'repo_2' is taken from repo_group that is defined in the config
        # it's required to run depsolving for whole repo_group, otherwise we
        # won't be able to find some deps that are not in the 'repo_1' but are
        # present in 'repo_2'
        # 'repo_not_allowed' is skipped completely
        mocked_apply_async.assert_called_once_with(
            args=[
                ["repo_1", "repo_2"],
            ]
        )

        # expected status code is 200
        assert response.status_code == 201
        json_data = response.json()
        # one task is expected to be spawned therefore there is only one item
        # in the response with proper task_id and state set
        assert len(json_data) == 1
        item = json_data[0]
        assert item["task_id"] == "foo-bar-id"
        assert item["state"] == "PENDING"


def test_manifest_post_not_allowed(client):
    """test request for depsolving for given repo ids, but none of the is allowed by config"""
    with mock.patch("celery.app.task.Task.apply_async") as mocked_apply_async:
        # none of repos in request are allowed for depsolving by config
        response = client.post(
            "/api/v1/manifest",
            json={"repo_ids": ["repo_not_allowed_1", "repo_not_allowed_2"]},
        )
        # we never call apply_async on depsolve_task
        mocked_apply_async.assert_not_called()
        # expected status code is 404
        assert response.status_code == 404
        # there is enough detail info in the response
        json_data = response.json()
        assert (
            json_data["detail"]
            == "None of ['repo_not_allowed_1', 'repo_not_allowed_2'] are allowed for depsolving."
        )


def test_manifest_delete(client):
    """test removing depsolved content for repository from redis"""
    depsolver_result_item = [
        {
            "src_repo_id": "source-foo-bar-repo",
            "unit_type": "RpmUnit",
            "unit_attr": "filename",
            "value": "some-filename.rpm",
        },
    ]

    depsolver_result_item_json_str = json.dumps(depsolver_result_item)

    redis_data = {"manifest:ubi_repo_id": depsolver_result_item_json_str}

    with mock.patch("ubi_manifest.app.api.redis.from_url") as mock_redis_from_url:
        mock_redis_from_url.return_value = MockedRedis(data=redis_data)
        # request deletion of ubi_repo_id entry
        response = client.delete("/api/v1/manifest/ubi_repo_id")

        # it should succeed
        assert response.status_code == 200
        assert response.json()["detail"] == "manifest:ubi_repo_id entry deleted"
        # and the entry is removed
        assert redis_data.get("ubi_repo_id") is None

        # second cal for removing the same entry returns 404
        response = client.delete(f"/api/v1/manifest/ubi_repo_id")
        assert response.status_code == 404
        assert response.json()["detail"] == "Content for ubi_repo_id not found"
