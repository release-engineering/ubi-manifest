import json
from unittest import mock

from attr import define

from .utils import MockedRedis, create_and_insert_repo, create_mock_configs


@define
class MockAsyncResult:
    task_id: str
    state: str


def test_status(client):
    response = client.get("/api/v1/status")

    assert response.status_code == 200
    assert response.json() == {"status": "OK"}


def test_task_state(client, auth_header):
    """test getting state of given celery task_id"""
    with mock.patch("ubi_manifest.app.api.app.AsyncResult") as task_mock:
        task_id = "some-task-id"
        task_mock.return_value = MockAsyncResult(task_id=task_id, state="PENDING")

        response = client.get(
            f"/api/v1/task/{task_id}", headers=auth_header(roles=["reader"])
        )

        # 200 status code is expected
        assert response.status_code == 200
        json_data = response.json()
        # task_id and state are properly set in response
        assert json_data["task_id"] == task_id
        assert json_data["state"] == "PENDING"


def test_task_state_not_found(client, auth_header):
    """test getting state of given celery task when task is not found"""
    with mock.patch("ubi_manifest.app.api.app.AsyncResult") as task_mock:
        task_id = "some-task-id"
        task_mock.return_value = None

        response = client.get(
            f"/api/v1/task/{task_id}", headers=auth_header(roles=["reader"])
        )
        # 404 is expected status code
        assert response.status_code == 404
        json_data = response.json()
        # proper detail is set in the response
        assert json_data["detail"] == f"Task {task_id} not found"


def test_manifest_get(client, auth_header):
    """test getting depsolved content for repository"""
    depsolver_result_item = [
        {
            "src_repo_id": "source-foo-bar-repo-1",
            "unit_type": "RpmUnit",
            "unit_attr": "filename",
            "value": "some-filename.rpm",
        },
        {
            "src_repo_id": "source-foo-bar-repo-2",
            "unit_type": "RpmUnit",
            "unit_attr": "filename",
            "value": "some-other-filename.rpm",
        },
    ]

    depsolver_result_item_json_str = json.dumps(depsolver_result_item)

    redis_data = {"ubi_repo_id": depsolver_result_item_json_str}

    with mock.patch("ubi_manifest.app.api.redis.from_url") as mock_redis_from_url:
        mock_redis_from_url.return_value = MockedRedis(data=redis_data)
        response = client.get(
            "/api/v1/manifest/ubi_repo_id", headers=auth_header(roles=["reader"])
        )

        # expected status code in 200
        assert response.status_code == 200
        json_data = response.json()
        # repo_id is set to the one we requested
        assert json_data["repo_id"] == "ubi_repo_id"

        content = sorted(json_data["content"], key=lambda x: x["value"])
        # there are two units in the content
        assert len(content) == 2
        content_item = content[0]
        # details of unit are set properly
        assert content_item["src_repo_id"] == "source-foo-bar-repo-1"
        assert content_item["unit_type"] == "RpmUnit"
        assert content_item["unit_attr"] == "filename"
        assert content_item["value"] == "some-filename.rpm"

        content_item = content[1]
        # details of unit are set properly
        assert content_item["src_repo_id"] == "source-foo-bar-repo-2"
        assert content_item["unit_type"] == "RpmUnit"
        assert content_item["unit_attr"] == "filename"
        assert content_item["value"] == "some-other-filename.rpm"


def test_manifest_get_empty(client, auth_header):
    """test getting empty manifest for repository"""
    depsolver_result_item = []
    depsolver_result_item_json_str = json.dumps(depsolver_result_item)

    redis_data = {"ubi_repo_id": depsolver_result_item_json_str}

    with mock.patch("ubi_manifest.app.api.redis.from_url") as mock_redis_from_url:
        mock_redis_from_url.return_value = MockedRedis(data=redis_data)
        response = client.get(
            "/api/v1/manifest/ubi_repo_id", headers=auth_header(roles=["reader"])
        )

        # expected status code in 200
        assert response.status_code == 200
        json_data = response.json()
        # repo_id is set to the one we requested
        assert json_data["repo_id"] == "ubi_repo_id"

        content = sorted(json_data["content"], key=lambda x: x["value"])
        # the content is empty
        assert len(content) == 0


def test_manifest_get_not_found(client, auth_header):
    """test getting depsolved content when the cotent is not available for given repo_id"""
    with mock.patch("ubi_manifest.app.api.redis.from_url") as mock_redis_from_url:
        mock_redis_from_url.return_value = MockedRedis(data={})
        response = client.get(
            "/api/v1/manifest/ubi_repo_id", headers=auth_header(roles=["reader"])
        )
        # expected status code is 404
        assert response.status_code == 404
        json_data = response.json()
        # response detail is properly set
        assert json_data["detail"] == "Content for ubi_repo_id not found"


@mock.patch("ubi_manifest.app.utils.ubiconfig.get_loader")
@mock.patch("ubi_manifest.worker.tasks.depsolver.utils.Client")
@mock.patch("celery.app.task.Task.apply_async")
def test_manifest_post_full_dep(
    mocked_apply_async, pulp_client, get_loader, client, pulp, auth_header
):
    """test request for depsolving for given repo ids where we use full depsolving"""
    mocked_apply_async.side_effect = [
        MockAsyncResult(task_id="foo-bar-id-1", state="PENDING"),
        MockAsyncResult(task_id="foo-bar-id-2", state="PENDING"),
    ]
    configs = create_mock_configs(3)
    get_loader.return_value = mock.Mock(load_all=mock.Mock(return_value=configs))
    create_and_insert_repo(
        id="ubi_repo_1",
        content_set="content_set_0",
        ubi_population=True,
        arch="arch1",
        pulp=pulp,
    )
    create_and_insert_repo(
        id="ubi_repo_2",
        content_set="content_set_1",
        ubi_population=True,
        arch="arch1",
        pulp=pulp,
    )
    create_and_insert_repo(
        id="ubi_repo_3",
        content_set="content_set_2",
        ubi_population=True,
        arch="arch2",
        pulp=pulp,
    )
    pulp_client.return_value = pulp.client

    requested_repos = ["ubi_repo_1", "ubi_repo_2", "ubi_repo_3", "ubi_repo_not_allowed"]
    response = client.post(
        "/api/v1/manifest",
        json={"repo_ids": requested_repos},
        headers=auth_header(roles=["creator"]),
    )
    # This will request two depsolve task:
    # First task for 'ubi_repo_1' and 'ubi_repo_2' because they are in one repo group determined by
    # get_items_for_depsolving() (repos are grouped by version-arch combinations).
    # Second task for 'ubi_repo_3' because this repo is in another repo group.
    # 'ubi_repo_not_allowed' will be skipped - not present in any repo_group.
    # It's required to run depsolving for the whole repo_group, otherwise we
    # won't be able to find some deps that are not in the 'ubi_repo_1' but are
    # present in 'ubi_repo_2'.
    mocked_apply_async.assert_has_calls(
        [
            mock.call(args=[["ubi_repo_1", "ubi_repo_2"], "url_or_dir_1"]),
            mock.call(args=[["ubi_repo_3"], "url_or_dir_1"]),
        ]
    )
    # expected status code is 201
    assert response.status_code == 201
    json_data = response.json()
    # two tasks are expected to be spawned
    assert len(json_data) == 2
    assert json_data[0]["task_id"] == "foo-bar-id-1"
    assert json_data[0]["state"] == "PENDING"
    assert json_data[1]["task_id"] == "foo-bar-id-2"
    assert json_data[1]["state"] == "PENDING"


@mock.patch("ubi_manifest.app.utils.ubiconfig.get_loader")
@mock.patch("ubi_manifest.worker.tasks.depsolver.utils.Client")
@mock.patch("celery.app.task.Task.apply_async")
def test_manifest_post_not_full_dep(
    mocked_apply_async, pulp_client, get_loader, client, pulp, auth_header
):
    """test request for depsolving for given repo ids where we do not use full depsolving"""
    mocked_apply_async.side_effect = [
        MockAsyncResult(task_id="foo-bar-id-1", state="PENDING"),
        MockAsyncResult(task_id="foo-bar-id-2", state="PENDING"),
    ]
    configs = create_mock_configs(
        2, flags=[{"base_pkgs_only": True}, {"base_pkgs_only": True}]
    )
    get_loader.return_value = mock.Mock(load_all=mock.Mock(return_value=configs))
    create_and_insert_repo(
        id="client-tools_repo_1",
        content_set="content_set_0",
        ubi_population=True,
        arch="arch1",
        pulp=pulp,
    )
    create_and_insert_repo(
        id="client-tools_repo_2",
        content_set="content_set_1",
        ubi_population=True,
        arch="arch1",
        pulp=pulp,
    )
    pulp_client.return_value = pulp.client

    response = client.post(
        "/api/v1/manifest",
        json={"repo_ids": ["client-tools_repo_1", "client-tools_repo_2"]},
        headers=auth_header(roles=["creator"]),
    )
    # This will request two depsolve tasks - one for each given repo.
    # For these repos we do not use full depsolving, so no groups needs to be
    # determined and the depsolving is performed separately for each repo.
    mocked_apply_async.assert_has_calls(
        [
            mock.call(args=[["client-tools_repo_1"], "url_or_dir_2"]),
            mock.call(args=[["client-tools_repo_2"], "url_or_dir_2"]),
        ]
    )
    # expected status code is 201
    assert response.status_code == 201
    json_data = response.json()
    # two tasks are expected to be spawned
    assert len(json_data) == 2
    assert json_data[0]["task_id"] == "foo-bar-id-1"
    assert json_data[0]["state"] == "PENDING"
    assert json_data[1]["task_id"] == "foo-bar-id-2"
    assert json_data[1]["state"] == "PENDING"


@mock.patch("ubi_manifest.app.utils.ubiconfig.get_loader")
@mock.patch("ubi_manifest.worker.tasks.depsolver.utils.Client")
@mock.patch("celery.app.task.Task.apply_async")
def test_manifest_post_no_depsolve_items(
    mocked_apply_async, pulp_client, get_loader, client, pulp, auth_header
):
    """test request for depsolving for given repo ids, but no depsolve items are identified"""
    get_loader.return_value = mock.Mock(
        load_all=mock.Mock(return_value=create_mock_configs(3))
    )
    create_and_insert_repo(
        id="ubi_repo_1",
        content_set="content_set_0",
        ubi_population=True,
        arch="arch1",
        pulp=pulp,
    )
    pulp_client.return_value = pulp.client

    response = client.post(
        "/api/v1/manifest",
        json={"repo_ids": ["ubi_repo_not_allowed"]},
        headers=auth_header(roles=["creator"]),
    )
    # No depsolve tasks are identified because 'ubi_repo_not_allowed' is not
    # found in Pulp (therefore not present in any repo group), so
    # we never call apply_async.
    mocked_apply_async.assert_not_called()
    # expected status code is 404
    assert response.status_code == 404
    # there is enough detail info in the response
    json_data = response.json()
    assert (
        json_data["detail"]
        == "No depsolve items were identified for ['ubi_repo_not_allowed']."
    )


@mock.patch("ubi_manifest.app.utils.ubiconfig.get_loader")
@mock.patch("ubi_manifest.worker.tasks.depsolver.utils.Client")
@mock.patch("celery.app.task.Task.apply_async")
def test_manifest_post_more_repo_classes(
    mocked_apply_async, pulp_client, get_loader, client, auth_header
):
    """test request for depsolving for given repo ids, which are from different repo classes"""
    response = client.post(
        "/api/v1/manifest",
        json={"repo_ids": ["ubi_repo", "client-tools_repo"]},
        headers=auth_header(roles=["creator"]),
    )
    # The request has finished before any calls on pulp client or ubiconfig were made because
    # repos from two different classes were in the request.
    mocked_apply_async.assert_not_called()
    pulp_client.assert_not_called()
    get_loader.assert_not_called()
    # expected status code is 400
    assert response.status_code == 400
    # there is enough detail info in the response
    json_data = response.json()
    assert (
        json_data["detail"]
        == "Can't process repos from different classes ['ubi', 'client-tools'] in one request. "
        "Please make separate request for each class."
    )


@mock.patch("ubi_manifest.app.utils.ubiconfig.get_loader")
@mock.patch("ubi_manifest.worker.tasks.depsolver.utils.Client")
@mock.patch("celery.app.task.Task.apply_async")
def test_manifest_post_wrong_repo_ids(
    mocked_apply_async, pulp_client, get_loader, client, auth_header
):
    """test request for depsolving for given repo ids, which are unexpected."""
    response = client.post(
        "/api/v1/manifest",
        json={"repo_ids": ["some_foreign_repo"]},
        headers=auth_header(roles=["creator"]),
    )
    # The request has finished before any calls on pulp client or ubiconfig were made because
    # repos from some unknown class were in the request.
    mocked_apply_async.assert_not_called()
    pulp_client.assert_not_called()
    get_loader.assert_not_called()
    # expected status code is 404
    assert response.status_code == 404
    # there is enough detail info in the response
    json_data = response.json()
    assert (
        json_data["detail"]
        == "Given repos ['some_foreign_repo'] have unexpected ids. It seems they are not "
        "from any of the accepted repo classes ['ubi', 'client-tools'] defined in content config."
    )


@mock.patch("ubi_manifest.app.utils.ubiconfig.get_loader")
@mock.patch("ubi_manifest.worker.tasks.depsolver.utils.Client")
@mock.patch("celery.app.task.Task.apply_async")
def test_manifest_post_no_repo_ids(
    mocked_apply_async, pulp_client, get_loader, client, auth_header
):
    """test request for depsolving for empty list of repo ids"""
    response = client.post(
        "/api/v1/manifest",
        json={"repo_ids": []},
        headers=auth_header(roles=["creator"]),
    )
    # The request has finished before any calls on pulp client or ubiconfig were made because
    # no repos were provided in the request.
    mocked_apply_async.assert_not_called()
    pulp_client.assert_not_called()
    get_loader.assert_not_called()
    # expected status code is 400
    assert response.status_code == 400
    # there is enough detail info in the response
    json_data = response.json()
    assert json_data["detail"] == "No repo IDs were provided."
