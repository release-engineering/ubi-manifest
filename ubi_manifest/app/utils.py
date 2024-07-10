import logging
from typing import Any

import ubiconfig
from pubtools.pulplib import Client, Criteria

from ubi_manifest.worker.utils import make_pulp_client

_LOG = logging.getLogger(__name__)


class FlagInconsistencyError(ValueError):
    pass


def get_repo_classes(content_config: dict[str, str], repo_ids: list[str]) -> list[str]:
    """
    Returns repo classes of repos for which the manifest creation was requested.
    """
    repo_classes = []
    for repo_class in content_config:
        for repo_id in repo_ids:
            if repo_class in repo_id:
                repo_classes.append(repo_class)
                break

    return repo_classes


def get_items_for_depsolving(
    app_conf: Any, repo_ids: list[str], repo_class: str
) -> list[dict[str, Any]]:
    """
    Returns a list of {"repo_group": ["repo1", "repo2"], "url": "https://config"}
    items which are then used for creation of depsolving tasks.
    """
    config_url = app_conf.content_config[repo_class]
    if app_conf.allowed_ubi_repo_groups:
        items = get_items_from_groups(
            repo_ids, app_conf.allowed_ubi_repo_groups, config_url
        )
    else:
        with make_pulp_client(app_conf) as client:
            configs = get_configs(config_url)
            base_pkg_only = check_and_get_flag(configs, config_url)
            if base_pkg_only:
                items = get_items_not_full_depsolving(
                    client, configs, repo_ids, config_url
                )
            else:
                repo_groups = get_repo_groups(client, configs)
                items = get_items_from_groups(repo_ids, repo_groups, config_url)

    _LOG.info("Determined items for depsolving: %s", items)
    return items


def get_items_from_groups(
    repo_ids: list[str], repo_groups: dict[str, set[str]], config_url: str
) -> list[dict[str, Any]]:
    """
    Returns a list of items for depsolving based on the given repo groups.
    """
    items: list[dict[str, Any]] = []
    needed_repo_groups: dict[str, set[str]] = {}

    for repo_id in repo_ids:
        for group_key, repo_group in repo_groups.items():
            if repo_id in repo_group:
                needed_repo_groups.setdefault(group_key, repo_group)
                break
    # Create items for depsolving
    for repo_group in needed_repo_groups.values():
        items.append({"repo_group": list(sorted(repo_group)), "url": config_url})

    return items


def get_configs(url: str) -> Any:
    """
    Returns configs from the given url.
    """
    _LOG.info("Loading config from %s", url)

    loader = ubiconfig.get_loader(url)
    configs = loader.load_all()
    # Use only configs for major versions
    configs = [conf for conf in configs if "." not in conf.version]

    return configs


def check_and_get_flag(configs: list[Any], url: str) -> Any:
    """
    Checks if all given configs have the same value in the 'base_pkgs_only' flag
    and returns that value if they do. Otherwise throws a FlagInconsistencyError.
    """
    flags = {config.flags.as_dict().get("base_pkgs_only", False) for config in configs}
    if len(flags) != 1:
        _LOG.error("Some config from %s has an unexpected 'base_pkg_only' flag.", url)
        raise FlagInconsistencyError()

    return list(flags)[0]


def get_items_not_full_depsolving(
    client: Client, configs: list[Any], repo_ids: list[str], config_url: str
) -> list[dict[str, Any]]:
    """
    Returns a list of items for depsolving for repos where we don't use full depsolving.
    Therefore no groups of repos need to be determined.
    """
    items: list[dict[str, Any]] = []
    pulp_repo_ids = set()

    # Get all repo ids from Pulp associated with the content sets from configs
    for config in configs:
        repos = get_repo_ids_from_cs(client, config.content_sets.rpm.output)
        pulp_repo_ids.update({r.id for r in repos})
    # Check that the provided repo ids are present among the repos from Pulp
    # and create items for depsolving for each present repo
    for repo_id in repo_ids:
        if repo_id in pulp_repo_ids:
            items.append({"repo_group": [repo_id], "url": config_url})
    return items


def get_repo_groups(client: Client, configs: list[Any]) -> dict[str, set[str]]:
    """
    Determines the allowed repo groups based on the the available ubi config and Pulp data.
    """
    repo_groups: dict[str, set[str]] = {}

    for config in configs:
        repos = get_repo_ids_from_cs(client, config.content_sets.rpm.output)
        for repo in repos:
            # Create groups by version and architecture
            group_key = f"{config.version}-{repo.arch}"
            repo_groups.setdefault(group_key, set()).add(repo.id)

    return repo_groups


def get_repo_ids_from_cs(client: Client, ubi_binary_cs: str) -> Any:
    """
    Returns a list of YumRepositories associated with the given content set
    and which have the ubi_population note set to True.
    """
    repos = client.search_repository(
        Criteria.and_(
            Criteria.with_field("notes.content_set", ubi_binary_cs),
            Criteria.with_field("ubi_population", True),
        )
    )
    return repos
