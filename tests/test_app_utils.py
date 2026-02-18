from unittest.mock import Mock, patch

import pytest

from ubi_manifest.app import utils

from .utils import create_and_insert_repo, create_mock_configs


@patch("ubi_manifest.app.utils.load_data")
@patch("ubi_manifest.app.utils.app")
def test_get_content_config_paths_from_cdn_definitions(mock_app, load_data):
    mock_app.conf.cdn_definitions_env = "ci"
    load_data.return_value = {
        "repo_content_sync": {
            "ci": [{"source": "foo"}, {"source": "bar"}],
            "xx": [{"source": "baz"}],
        }
    }

    result = utils.get_content_config_paths()

    assert result == ["foo", "bar"]


@patch("ubi_manifest.app.utils.app")
def test_get_content_config_paths_from_content_config(mock_app):
    mock_app.conf.cdn_definitions_url = None
    mock_app.conf.content_config = {"ubi": "baz", "client-tools": "quux"}

    result = utils.get_content_config_paths()

    assert result == ["baz", "quux"]


def test_get_items_from_groups():
    repo_groups = {
        "7-aarch64": {"ubi_repo1", "ubi_repo2", "ubi_repo3"},
        "8-aarch64": {"ubi_repo4", "ubi_repo5", "ubi_repo6"},
        "8-x86_64": {"ubi_repo7", "ubi_repo8", "ubi_repo9"},
    }
    repo_ids = ["ubi_repo4", "ubi_repo5", "ubi_repo9"]

    result = utils.get_items_from_groups(repo_ids, repo_groups, "https://ubi")

    assert result == [
        {
            "repo_group": ["ubi_repo4", "ubi_repo5", "ubi_repo6"],
            "url": "https://ubi",
        },
        {
            "repo_group": ["ubi_repo7", "ubi_repo8", "ubi_repo9"],
            "url": "https://ubi",
        },
    ]


@patch("ubiconfig.get_loader")
def test_get_configs(get_loader):
    conf1 = Mock(version="8")
    conf2 = Mock(version="8.9")
    get_loader.return_value = Mock(load_all=Mock(return_value=[conf1, conf2]))

    result = utils.get_configs("https://ubi")
    assert result == [conf1]


def test_check_and_get_flag():
    configs = create_mock_configs(2)
    result = utils.check_and_get_flag(configs, "url")
    assert result is False


def test_check_and_get_flag_error():
    configs = create_mock_configs(2)
    configs[1].flags.as_dict.return_value = {"base_pkgs_only": True}
    # 'base_pkg_only' flag is expected to have same value in all configs for one repo class
    with pytest.raises(utils.FlagInconsistencyError):
        utils.check_and_get_flag(configs, "url")


def test_get_repo_groups_default_repos_only(pulp):
    """
    Test for mainline/default repos only.
    """
    configs = create_mock_configs(4)
    create_and_insert_repo(
        id="ubi8_repo1_for_aarch64",
        content_set="content_set_0",
        ubi_population=True,
        arch="aarch64",
        pulp=pulp,
    )
    create_and_insert_repo(
        id="ubi8_repo2_for_aarch64",
        content_set="content_set_1",
        ubi_population=True,
        arch="aarch64",
        pulp=pulp,
    )
    create_and_insert_repo(
        id="ubi8_repo3_for_aarch64",
        content_set="content_set_2",
        ubi_population=False,
        arch="aarch64",
        pulp=pulp,
    )
    create_and_insert_repo(
        id="ubi8_repo1_for_x86_64",
        content_set="content_set_3",
        ubi_population=True,
        arch="x86_64",
        pulp=pulp,
    )

    result = utils.get_repo_groups(pulp.client, configs)
    assert result == {
        "8-aarch64-default": {"ubi8_repo1_for_aarch64", "ubi8_repo2_for_aarch64"},
        "8-x86_64-default": {"ubi8_repo1_for_x86_64"},
    }


def test_get_repo_groups_mixed_dot_repos(pulp):
    """
    Test for mixed dot and mainline/default repos.
    """
    configs = create_mock_configs(2)
    create_and_insert_repo(
        id="ubi8_repo1_for_aarch64",
        content_set="content_set_0",
        ubi_population=True,
        arch="aarch64",
        pulp=pulp,
    )
    create_and_insert_repo(
        id="ubi8_repo2_for_aarch64",
        content_set="content_set_1",
        ubi_population=True,
        arch="aarch64",
        pulp=pulp,
    )
    create_and_insert_repo(
        id="ubi8_repo1_for_aarch64__8_DOT_0",
        content_set="content_set_0",
        ubi_population=True,
        arch="aarch64",
        pulp=pulp,
    )
    create_and_insert_repo(
        id="ubi8_repo2_for_aarch64__8_DOT_0",
        content_set="content_set_1",
        ubi_population=True,
        arch="aarch64",
        pulp=pulp,
    )

    result = utils.get_repo_groups(pulp.client, configs)
    assert result == {
        "8-aarch64-default": {"ubi8_repo1_for_aarch64", "ubi8_repo2_for_aarch64"},
        "8-aarch64-0": {"ubi8_repo1_for_aarch64__8_DOT_0", "ubi8_repo2_for_aarch64__8_DOT_0"},
    }

@pytest.mark.parametrize(
    "definitions_path,configs_path,expected_result",
    [
        (
            "https://gitlab.com/cdn-definitions.yaml",
            [],
            "https://gitlab.com/-/health",
        ),
        (
            "/path/to/cdn-definitions.yaml",
            ["https://gitlab.com/ubi", "https://gitlab.com/client-tools"],
            "https://gitlab.com/-/health",
        ),
        (
            "/path/to/cdn-definitions.yaml",
            ["/path/to/ubi/", ".path/to/client-tools/"],
            None,
        ),
    ],
)
@patch("ubi_manifest.app.utils.get_content_config_paths")
@patch("ubi_manifest.app.utils.app")
def test_get_gitlab_healthcheck_url(
    mock_app,
    mock_get_content_config_paths,
    definitions_path,
    configs_path,
    expected_result,
):
    mock_app.conf.cdn_definitions_url = definitions_path
    mock_get_content_config_paths.return_value = configs_path
    result = utils.get_gitlab_healthcheck_url()
    assert result == expected_result
