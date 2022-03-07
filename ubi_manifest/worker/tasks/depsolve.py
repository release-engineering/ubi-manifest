import logging
from typing import Dict, List

from ubi_manifest.worker.tasks.celery import app
from ubi_manifest.worker.tasks.depsolver.models import DepsolverItem, UbiUnit
from ubi_manifest.worker.tasks.depsolver.rpm_depsolver import Depsolver
from ubi_manifest.worker.tasks.depsolver.ubi_config import UbiConfigLoader
from ubi_manifest.worker.tasks.depsolver.utils import (
    make_pulp_client,
    remap_keys,
    split_filename,
)

_LOG = logging.getLogger(__name__)


@app.task
def depsolve_task(ubi_repo_ids: List[str]) -> Dict[str, List[UbiUnit]]:
    """
    Run depsolvers for given ubi_repo_ids. Debuginfo repos related to those provide
    as parameter are automatically resolved as well. Returns a dictionary where key is
    a destination ubi_repo_id, value is a list of UbiUnit that should appear in
    the repository.
    """
    ubi_config_loader = UbiConfigLoader(app.conf["ubi_config_url"])

    with make_pulp_client(
        app.conf["pulp_url"],
        app.conf["pulp_username"],
        app.conf["pulp_password"],
        app.conf["pulp_insecure"],
    ) as client:
        repos_map = {}
        debuginfo_dep_map = {}
        dep_map = {}

        for ubi_repo_id in ubi_repo_ids:
            repo = client.get_repository(ubi_repo_id)
            debuginfo_repo = repo.get_debug_repository()
            version = repo.ubi_config_version
            # get proper ubi_config for given content_set and version
            config = ubi_config_loader.get_config(repo.content_set, version)
            # config not found for specific version, try to fallback to default version
            if config is None:
                config = ubi_config_loader.get_config(
                    repo.content_set, version.split(".")[0]
                )
            # create rhel_repo:ubi_repo mapping
            for _repo, sources in zip(
                [repo, debuginfo_repo],
                [repo.population_sources, debuginfo_repo.population_sources],
            ):
                for item in sources:
                    repos_map[item] = _repo.id

            whitelist, debuginfo_whitelist = _filter_whitelist(config)

            dep_map[repo.id] = _make_depsolver_item(client, repo, whitelist)
            debuginfo_dep_map[debuginfo_repo.id] = _make_depsolver_item(
                client, debuginfo_repo, debuginfo_whitelist
            )
        # run depsolver for binary repos
        _LOG.info("Running depsolver for RPM repos: %s", list(dep_map.keys()))
        out = _run_depsolver(list(dep_map.values()), repos_map)

        # generate missing debuginfo packages
        # TODO this seems to generate too many debuginfo packages - fix after tests with real data
        for ubi_repo_id, pkg_list in out.items():
            debuginfo_to_add = set()
            for pkg in pkg_list:
                # inspired with pungi depsolver
                source_name = split_filename(pkg.sourcerpm)[0]
                debuginfo_to_add.add(f"{pkg.name}-debuginfo")
                debuginfo_to_add.add(f"{source_name}-debugsource")

            _id = client.get_repository(ubi_repo_id).get_debug_repository().id
            # update whitelist for given ubi depsolver item
            debuginfo_dep_map[_id].whitelist.update(debuginfo_to_add)

        # run depsolver for debuginfo repo
        _LOG.info(
            "Running depsolver for DEBUGINFO repos: %s", list(debuginfo_dep_map.keys())
        )
        debuginfo_out = _run_depsolver(list(debuginfo_dep_map.values()), repos_map)

    out.update(debuginfo_out)
    return out


def _filter_whitelist(ubi_config):
    whitelist = set()
    debuginfo_whitelist = set()

    for pkg in ubi_config.packages.whitelist:
        if pkg.arch == "src":
            continue
        if pkg.name.endswith("debuginfo") or pkg.name.endswith("debugsource"):
            debuginfo_whitelist.add(pkg.name)
        else:
            whitelist.add(pkg.name)

    return whitelist, debuginfo_whitelist


def _make_depsolver_item(client, repo, whitelist):
    in_pulp_repos = _get_population_sources(client, repo)
    return DepsolverItem(whitelist, in_pulp_repos)


def _get_population_sources(client, repo):
    return [client.get_repository(repo_id) for repo_id in repo.population_sources]


def _run_depsolver(depolver_items, repos_map):
    with Depsolver(depolver_items) as depsolver:
        depsolver.run()
        exported = depsolver.export()
        out = remap_keys(repos_map, exported)
    return out
