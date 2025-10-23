from unittest.mock import Mock, patch

import pytest
from pubtools.pulplib import YumRepository

from ubi_manifest.app import utils

from .utils import create_and_insert_repo, create_mock_configs


@pytest.mark.parametrize(
    "repo_ids,expected_result",
    [
        (["ubi_repo1, ubi_repo2"], ["ubi"]),
        (["ubi_repo, client-tools_repo"], ["ubi", "client-tools"]),
        (["foreign_repo"], []),
    ],
)
def test_get_repo_classes(repo_ids, expected_result):
    content_config = {"ubi": "https://ubi", "client-tools": "https://ct"}
    result = utils.get_repo_classes(content_config, repo_ids)
    assert result == expected_result


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
    configs = create_mock_configs(2, flags=[{}, {"base_pkgs_only": True}])
    # 'base_pkg_only' flag is expected to have same value in all configs for one repo class
    with pytest.raises(utils.FlagInconsistencyError):
        utils.check_and_get_flag(configs, "url")


def test_get_repo_groups(pulp):
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
        "8-aarch64": {"ubi8_repo1_for_aarch64", "ubi8_repo2_for_aarch64"},
        "8-x86_64": {"ubi8_repo1_for_x86_64"},
    }


@pytest.mark.parametrize(
    "config,expected_result",
    [
        (
            {
                "ubi": "https://gitlab.com/ubi",
                "client-tools": "https://gitlab.com/client-tools",
            },
            "https://gitlab.com",
        ),
        ({"ubi": "/path/to/ubi/", "client-tools": ".path/to/client-tools/"}, None),
    ],
)
def test_get_gitlab_base_url(config, expected_result):
    result = utils.get_gitlab_base_url(config)
    assert result == expected_result
