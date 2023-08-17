import json
import logging
from typing import Dict, List
from collections import defaultdict

import redis
from pubtools.pulplib import ModulemdDefaultsUnit, ModulemdUnit, RpmUnit

from ubi_manifest.worker.tasks.celery import app
from ubi_manifest.worker.tasks.depsolver.models import (
    DepsolverItem,
    ModularDepsolverItem,
    UbiUnit,
)
from ubi_manifest.worker.tasks.depsolver.modulemd_depsolver import ModularDepsolver
from ubi_manifest.worker.tasks.depsolver.rpm_depsolver import Depsolver
from ubi_manifest.worker.tasks.depsolver.ubi_config import UbiConfigLoader
from ubi_manifest.worker.tasks.depsolver.utils import (
    make_pulp_client,
    parse_blacklist_config,
    remap_keys,
    split_filename,
)

_LOG = logging.getLogger(__name__)


class ContentConfigMissing(Exception):
    pass


@app.task
def depsolve_task(ubi_repo_ids: List[str], content_config_url: str) -> None:
    """
    Run depsolvers for given ubi_repo_ids - it's expected that id of binary
    repositories are provided. Debuginfo and SRPM repos related to those ones
    provided as parameter are automatically resolved as well, because content
    of debuginfo and SRPM repos is dependent on the content of binary repo.
    Depsolved units are saved to redis - key is a destination repository id
    and value is a list of items, where item is a dict with keys:
    (source_repo_id, unit_type, unit_attr, value). Note that value in redis
    is stored as json string.
    """
    ubi_config_loader = UbiConfigLoader(content_config_url)

    with make_pulp_client(app.conf) as client:
        repos_map = {}
        debug_dep_map = {}
        dep_map = {}
        mod_dep_map = {}
        in_source_rpm_repos = []
        for ubi_repo_id in ubi_repo_ids:
            repo = client.get_repository(ubi_repo_id)
            debuginfo_repo = repo.get_debug_repository()
            srpm_repo = repo.get_source_repository()

            # create rhel_repo:ubi_repo mapping
            for _repo, sources in zip(
                [repo, debuginfo_repo, srpm_repo],
                [
                    repo.population_sources,
                    debuginfo_repo.population_sources,
                    srpm_repo.population_sources,
                ],
            ):
                for item in sources:
                    repos_map[item] = _repo.id

            cs_repo_map, cs_debug_repo_map = _get_population_sources_per_cs(
                client, repo
            )
            # if we have population sources with different content sets and different content configs
            # we need to make sure that we use correct config for each input repo
            for input_cs, input_repos in cs_repo_map.items():
                config = _get_content_config(
                    ubi_config_loader,
                    input_cs,
                    repo.content_set,
                    repo.ubi_config_version,
                )
                whitelist, debuginfo_whitelist = _filter_whitelist(config)
                blacklist = parse_blacklist_config(config)

                in_source_rpm_repos.extend(_get_population_sources(client, srpm_repo))

                dep_map[(repo.id, input_cs)] = DepsolverItem(
                    whitelist, blacklist, input_repos
                )

                debug_dep_map[(debuginfo_repo.id, input_cs)] = DepsolverItem(
                    debuginfo_whitelist,
                    blacklist,
                    cs_debug_repo_map[input_cs],
                )

                # modulemd depsolver vars
                modulelist = config.modules.whitelist
                mod_dep_map[(repo.id, input_cs)] = ModularDepsolverItem(
                    modulelist, repo, input_repos
                )

        # run modular depsolver
        _LOG.info(
            "Running MODULEMD depsolver for repos: %s",
            [item[0] for item in mod_dep_map.keys()],
        )
        modulemd_out = _run_modulemd_depsolver(list(mod_dep_map.values()), repos_map)
        out = modulemd_out["modules_out"]
        modulemd_rpm_deps = modulemd_out["rpm_dependencies"]

        # run depsolver for binary repos
        _LOG.info(
            "Running depsolver for RPM repos: %s", [item[0] for item in dep_map.keys()]
        )
        # TODO this blocks task from processing, depsolving of debuginfo packages
        # could be moved to the Depsolver. It should lead to more async processing
        # and better performance
        rpm_out = _run_depsolver(
            list(dep_map.values()),
            repos_map,
            in_source_rpm_repos,
            modulemd_rpm_deps,
        )

        _merge_output_dictionary(out, rpm_out)
        _update_debug_whitelist(client, out, debug_dep_map)

        # run depsolver for debuginfo repo
        _LOG.info(
            "Running depsolver for DEBUGINFO repos: %s",
            [item[0] for item in debug_dep_map.keys()],
        )
        debuginfo_out = _run_depsolver(
            list(debug_dep_map.values()),
            repos_map,
            in_source_rpm_repos,
            modulemd_rpm_deps,
        )
    # merge 'out' and 'debuginfo_out' dicts without overwriting any entry
    _merge_output_dictionary(out, debuginfo_out)

    # make sure that there are all ubi repositories in the 'out' dictionary set a keys
    # repositories with empty manifest are ommited from previous processing
    for repo_id in repos_map.values():
        if repo_id not in out:
            out[repo_id] = []
    # save depsolved data to redis
    _save(out)


def _update_debug_whitelist(client, output_set, debug_dep_map):
    # generate missing debuginfo packages
    # TODO this seems to generate too many debuginfo packages - fix after tests with real data
    for ubi_repo_id, pkg_list in output_set.items():
        debuginfo_to_add = set()
        for pkg in pkg_list:
            # inspired with pungi depsolver
            if pkg.isinstance_inner_unit(RpmUnit) and pkg.sourcerpm:
                source_name = split_filename(pkg.sourcerpm)[0]
                debuginfo_to_add.add(f"{pkg.name}-debuginfo")
                debuginfo_to_add.add(f"{source_name}-debugsource")

        _repo_out = client.get_repository(ubi_repo_id)
        _repo_debug_out = _repo_out.get_debug_repository()
        for _repo_in_id in _repo_debug_out.population_sources:
            # update whitelist for given ubi depsolver item
            rpm_in_repo = client.get_repository(_repo_in_id).get_binary_repository()
            debug_dep_map[
                (_repo_debug_out.id, rpm_in_repo.content_set)
            ].whitelist.update(debuginfo_to_add)


def _save(data: Dict[str, List[UbiUnit]]) -> None:
    redis_client = redis.from_url(app.conf.result_backend)

    data_for_redis = {}
    for repo_id, units in data.items():
        items = []
        for unit in units:
            if unit.isinstance_inner_unit(RpmUnit):
                item = {
                    "src_repo_id": unit.associate_source_repo_id,
                    "unit_type": "RpmUnit",
                    "unit_attr": "filename",
                    "value": unit.filename,
                }
            elif unit.isinstance_inner_unit(ModulemdUnit):
                item = {
                    "src_repo_id": unit.associate_source_repo_id,
                    "unit_type": "ModulemdUnit",
                    "unit_attr": "nsvca",
                    "value": unit.nsvca,
                }
            elif unit.isinstance_inner_unit(ModulemdDefaultsUnit):
                item = {
                    "src_repo_id": unit.associate_source_repo_id,
                    "unit_type": "ModulemdDefaultsUnit",
                    "unit_attr": "name:stream",
                    "value": f"{unit.name}:{unit.stream}",
                }
            items.append(item)

        data_for_redis[repo_id] = items
    # save data to redis as key:json_string
    for key, values in data_for_redis.items():
        redis_client.set(
            key, json.dumps(values), ex=app.conf["ubi_manifest_data_expiration"]
        )


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


def _get_population_sources(client, repo):
    return [client.get_repository(repo_id) for repo_id in repo.population_sources]


def _run_depsolver(depolver_items, repos_map, in_source_rpm_repos, modulemd_deps):
    with Depsolver(depolver_items, in_source_rpm_repos, modulemd_deps) as depsolver:
        depsolver.run()
        exported = depsolver.export()
        out = remap_keys(repos_map, exported)
    return out


def _run_modulemd_depsolver(modular_items, repos_map):
    with ModularDepsolver(modular_items) as depsolver:
        depsolver.run()
        out = depsolver.export()
        out["modules_out"] = remap_keys(repos_map, out["modules_out"])
    return out


def _merge_output_dictionary(out, update):
    """
    Appends to lists in out.values() instead of overwriting them
    WARNING: This works correctly only with RpmUnit values
    """
    for key, data in update.items():
        if key in out:
            filenames = [
                item.filename
                for item in out[key]
                # ModulemdUnits don't have filename attr.
                if item.isinstance_inner_unit(RpmUnit)
            ]
            for item in data:
                if item.filename not in filenames:
                    out[key].append(item)
        else:
            out[key] = data


def _get_population_sources_per_cs(client, repo):
    rpm_sources = defaultdict(list)
    debug_sources = defaultdict(list)
    for repo_id in repo.population_sources:
        input_rpm_repo = client.get_repository(repo_id)
        input_debug_repo = input_rpm_repo.get_debug_repository()

        # intentionally using input_rpm_repo.content_set as key in both dictionaries
        rpm_sources[input_rpm_repo.content_set].append(input_rpm_repo)
        debug_sources[input_rpm_repo.content_set].append(input_debug_repo)

    return rpm_sources, debug_sources


def _get_content_config(ubi_config_loader, input_cs, output_cs, version):
    out = None
    # get proper ubi_config for given input and output content sets and a version
    # fallback to default version if there is no match for requested config
    for _ver in (version, version.split(".")[0]):
        out = ubi_config_loader.get_config(input_cs, output_cs, _ver)
        if out:
            break

    if out is None:
        raise ContentConfigMissing

    return out
