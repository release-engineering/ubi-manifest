import logging
from typing import Any, Optional
from urllib.parse import urlparse

import ubiconfig
from cdn_definitions import load_data
from pubtools.pulplib import Client, Criteria

from ubi_manifest.worker.tasks.celery import app
from ubi_manifest.worker.utils import make_pulp_client

_LOG = logging.getLogger(__name__)


class FlagInconsistencyError(ValueError):
    pass


def get_content_config_paths() -> list[str]:
    """
    Returns a list of content config paths or URLs loaded from cdn-definitions
    or `content_config` configuration property.

    The definitions URL and environment are determined by `cdn_definitions_url`
    and `cdn_definitions_env` in app.conf. The paths or URLs are extracted from
    the 'repo_content_sync' key in the data returned by `cdn_definitions.load_data()`.
    If no `cdn_definitions_url` is set, the content config paths or URLs defined
    in `content_config` are returned instead.
    """
    if app.conf.cdn_definitions_url:
        _LOG.info(
            "Loading content config URLs for environment '%s' from '%s'",
            app.conf.cdn_definitions_env,
            app.conf.cdn_definitions_url,
        )
        urls = [
            item["source"]
            for item in load_data(app.conf.cdn_definitions_url)
            .get("repo_content_sync", {})
            .get(app.conf.cdn_definitions_env, [])
        ]
    else:
        urls = list(app.conf.content_config.values())
    _LOG.info("Loaded %d content config URL(s): %s", len(urls), urls)
    return urls

def get_content_configs() -> list[dict]:
    if app.conf.cdn_definitions_url:
        _LOG.info(
            "Loading content configs for environment '%s' from '%s'",
            app.conf.cdn_definitions_env,
            app.conf.cdn_definitions_url,
        )
        repo_contents = (load_data(app.conf.cdn_definitions_url)
                     .get("repo_content_sync", {})
                     .get(app.conf.cdn_definitions_env, []))
        content_configs = [
        {
            "source": repo_content.get("source", None),
            "branch_prefix": repo_content.get("branch_prefix", None),
            "populate_dot_repos": repo_content.get("populate_dot_repos", None)
        }
        for repo_content in repo_contents
    ]
    else:
        content_configs = [
            {
                "source": url
            }
            for url in list(app.conf.content_config.values())
        ]
    return content_configs

def get_items_for_depsolving(
    app_conf: Any, repo_ids: list[str]
) -> list[dict[str, Any]]:
    """
    Returns a list of {"repo_group": ["repo1", "repo2"], "url": "https://config"}
    items which are then used for creation of depsolving tasks.
    """
    all_items: list[dict[str, Any]] = []

    with make_pulp_client(app_conf) as client:
        # Try each config URL to find matching repos
        configs = get_content_configs()
        for config in configs:
            config_url = config.get("source")
            config_branch = config.get("branch_prefix", None)
            configs = get_configs(config_url, config_branch)
            if not configs:
                continue

            base_pkg_only = check_and_get_flag(configs, config_url)
            if base_pkg_only:
                items = get_items_not_full_depsolving(
                    client, configs, repo_ids, config_url
                )
            else:
                repo_groups = get_repo_groups(client, configs)
                items = get_items_from_groups(repo_ids, repo_groups, config_url)

            all_items.extend(items)

    _LOG.info("Determined items for depsolving: %s", all_items)
    return all_items


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


def get_configs(url: str, branch_prefix: str=None) -> Any:
    """
    Returns configs from the given url.
    """
    _LOG.info("Loading config from %s", url)

    loader = ubiconfig.get_loader(url, branch_prefix)
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


def get_gitlab_healthcheck_url() -> Optional[str]:
    """
    Returns GitLab healthcheck URL if either the CDN definitions or content configs
    are loaded from an URL, making an assumption that it is a GitLab URL.
    Returns None if all the configs are loaded from filesystem.
    """
    parsed = urlparse(app.conf.cdn_definitions_url)
    # If there is a scheme, CDN definitions are loaded from an URL, so
    # we assume it is a GitLab URL and return its healthcheck URL.
    if parsed.scheme:
        return f"{parsed.scheme}://{parsed.netloc}/-/health"

    # CDN definitions were loaded from a file, check if any sync repo is on GitLab
    for config_path in get_content_config_paths():
        parsed = urlparse(config_path)
        # If there is a scheme, content config is loaded from an URL, so
        # we assume it is a GitLab URL and return its healthcheck URL.
        if parsed.scheme:
            return f"{parsed.scheme}://{parsed.netloc}/-/health"
    # All configs are loaded from filesystem, therefore no healthcheck is needed.
    return None
