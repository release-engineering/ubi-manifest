import logging
from collections import defaultdict
from concurrent.futures import Future, as_completed
from itertools import chain
from typing import Any

from more_executors.futures import f_proxy
from pubtools.pulplib import Criteria, ModulemdDefaultsUnit, ModulemdUnit, RpmUnit

from ubi_manifest.worker.common import filter_whitelist, get_pkgs_from_all_modules
from ubi_manifest.worker.models import PackageToExclude, UbiUnit
from ubi_manifest.worker.pulp_queries import search_units
from ubi_manifest.worker.tasks.celery import app
from ubi_manifest.worker.ubi_config import UbiConfigLoader, get_content_config
from ubi_manifest.worker.utils import (
    RELATION_CMP_MAP,
    create_or_criteria,
    get_criteria_for_modules,
    is_blacklisted,
    keep_n_latest_modules,
    keep_n_latest_rpms,
    make_pulp_client,
    parse_blacklist_config,
)

_LOG = logging.getLogger(__name__)

RPM_FIELDS = ["name", "version", "release", "arch", "filename"]
MD_FIELDS = ["name", "stream", "version", "context", "arch"]


@app.task  # type: ignore [misc]  # ignore untyped decorator
def content_audit_task() -> None:
    """
    This task checks that all available content is up-to-date, that whitelisted
    content is present, and that blacklisted content is absent.
    """

    config_loaders = [UbiConfigLoader(url) for url in app.conf.content_config.values()]

    with make_pulp_client(app.conf) as client:
        for out_repo in client.search_repository(
            Criteria.with_field("ubi_population", True)
        ):
            # we can skip modulemd/modulemd_defaults bits for debug or source repos
            has_modules = all(s not in out_repo.id for s in ("debug", "source"))

            # get all relevant units currently on output repo
            out_rpms = f_proxy(
                search_units(
                    out_repo, [Criteria.true()], RpmUnit, unit_fields=RPM_FIELDS
                )
            )
            if has_modules:
                out_mds = f_proxy(
                    search_units(
                        out_repo, [Criteria.true()], ModulemdUnit, unit_fields=MD_FIELDS
                    )
                )
                out_mdds = f_proxy(
                    search_units(out_repo, [Criteria.true()], ModulemdDefaultsUnit)
                )

            seen_rpms: set[UbiUnit] = set()
            seen_modules: set[str] = set()
            output_whitelist: set[str] = set()
            output_blacklist: list[PackageToExclude] = []

            in_rpms_fts: list[Future[set[UbiUnit]]] = []
            in_mds_fts: list[Future[set[UbiUnit]]] = []
            in_mdds_fts: list[Future[set[UbiUnit]]] = []

            in_repos = client.search_repository(
                Criteria.with_id(out_repo.population_sources)
            )
            if has_modules:
                modular_rpm_filenames = get_pkgs_from_all_modules(
                    list(in_repos) + [out_repo]
                )

            for in_repo in in_repos:
                # get all corresponding units currently on input repo
                in_rpms_fts.append(
                    search_units(
                        in_repo,
                        _get_criteria_for_rpms(out_rpms),
                        RpmUnit,
                        None,
                        RPM_FIELDS,
                    )
                )
                if has_modules:
                    in_mds_fts.append(
                        search_units(
                            in_repo,
                            get_criteria_for_modules(out_mds),  # type: ignore [arg-type]
                            ModulemdUnit,
                            None,
                            MD_FIELDS,
                        )
                    )
                    in_mdds_fts.append(
                        search_units(
                            in_repo,
                            get_criteria_for_modules(out_mdds),  # type: ignore [arg-type]
                            ModulemdDefaultsUnit,
                        )
                    )

                # accumulate input repo white/blacklists
                for loader in config_loaders:
                    config = get_content_config(
                        loader,
                        in_repo.content_set,
                        out_repo.content_set,
                        out_repo.ubi_config_version,
                    )
                    output_blacklist.extend(parse_blacklist_config(config))
                    pkg_whitelist, debuginfo_whitelist = filter_whitelist(
                        config, output_blacklist
                    )
                    output_whitelist |= pkg_whitelist
                    if "debug" in out_repo.id:
                        output_whitelist |= debuginfo_whitelist
                    if has_modules:
                        output_whitelist |= {
                            f"{md.name}:{md.stream}" for md in config.modules.whitelist
                        }

            # check that all content is up-to-date
            out_rpms_result = out_rpms.result()
            for in_rpm in _latest_input_rpms(in_rpms_fts):
                if has_modules and in_rpm.filename in modular_rpm_filenames:
                    _LOG.debug(
                        "[%s] Skipping modular RPM %s", out_repo.id, in_rpm.filename
                    )
                    # record seen modular RPMs as modules since they may be in module whitelist
                    seen_modules.add(f"{in_rpm.name}:{in_rpm.version}")
                    continue
                for out_rpm in out_rpms_result.copy():
                    if has_modules and out_rpm.filename in modular_rpm_filenames:
                        # skip modular RPMs from out_repo also
                        out_rpms_result.discard(out_rpm)
                        continue
                    if (out_rpm.name, out_rpm.arch) == (in_rpm.name, in_rpm.arch):
                        _compare_versions(out_repo.id, out_rpm, in_rpm)
                        seen_rpms.add(in_rpm)
                        out_rpms_result.discard(out_rpm)
                        break
            if has_modules:
                out_mds_result = out_mds.result()
                for in_md in _latest_input_mds(in_mds_fts):
                    for out_md in out_mds_result.copy():
                        if (out_md.name, out_md.stream) == (in_md.name, in_md.stream):
                            _compare_versions(out_repo.id, out_md, in_md)
                            seen_modules.add(f"{in_md.name}:{in_md.stream}")
                            out_mds_result.discard(out_md)
                            break
                out_mdds_result = out_mdds.result()
                for in_mdd in chain.from_iterable(
                    ft.result() for ft in as_completed(in_mdds_fts)
                ):
                    for out_mdd in out_mdds_result.copy():
                        if out_mdd.name == in_mdd.name:
                            _compare_versions(out_repo.id, out_mdd, in_mdd)
                            out_mdds_result.discard(out_mdd)
                            break

            # check seen RPMs against blacklist
            if blacklisted := {
                u.name for u in seen_rpms if is_blacklisted(u, output_blacklist)
            }:
                _LOG.warning(
                    "[%s] blacklisted content found in input repositories;\n\t%s",
                    out_repo.id,
                    "\n\t".join(sorted(blacklisted)),
                )

            # check seen RPMs and Modules off of whitelist
            to_check = {u.name for u in seen_rpms} | seen_modules
            _LOG.debug(
                "[%s] checking following seen units against whitelist;\n\t%s",
                out_repo.id,
                "\n\t".join(to_check),
            )
            for pattern in output_whitelist.copy():
                if matches := {name for name in to_check if pattern in name}:
                    output_whitelist.remove(pattern)
                    # Let's not recheck those we've already found
                    to_check -= matches

            # report any missing whitelisted packages for the output repo
            if output_whitelist:
                _LOG.warning(
                    "[%s] whitelisted content missing from UBI and/or population sources;\n\t%s",
                    out_repo.id,
                    "\n\t".join(sorted(output_whitelist)),
                )


def _get_criteria_for_rpms(output_rpms: Future[set[UbiUnit]]) -> list[Criteria]:
    fields = ["name", "arch"]
    values = [(rpm.name, rpm.arch) for rpm in output_rpms]  # type: ignore [attr-defined]
    return create_or_criteria(fields, values)


def _latest_input_rpms(fts: list[Future[set[UbiUnit]]]) -> list[UbiUnit]:
    # unit set is expected to contain a variety of RPMs, so we'll have to group by name+arch
    # before finding latest of each
    latest_rpms = []
    rpm_map = defaultdict(list)

    for ft in as_completed(fts):
        for rpm in ft.result():
            rpm_map[f"{rpm.name}_{rpm.arch}"].append(rpm)
    for rpm_group in rpm_map.values():
        keep_n_latest_rpms(rpm_group)
        latest_rpms.extend(rpm_group)

    return latest_rpms


def _latest_input_mds(fts: list[Future[set[UbiUnit]]]) -> list[UbiUnit]:
    latest_mds = []
    module_map = defaultdict(list)

    for ft in as_completed(fts):
        for md in ft.result():
            module_map[f"{md.name}:{md.stream}"].append(md)
    for module_group in module_map.values():
        module_group.sort(key=lambda module: module.version)
        keep_n_latest_modules(module_group)
        latest_mds.extend(module_group)

    return latest_mds


def _compare_versions(repo_id: str, out_unit: UbiUnit, in_unit: UbiUnit) -> None:
    """
    Compares RpmUnits and ModulemdUnits by version and ModulemdDefaultsUnits by
    profile equality, logging a warning if input is more recent than output.
    """

    def log_warning(warn_tuple: tuple[str, Any, Any]) -> None:
        _LOG.warning(
            "[%s] UBI %s '%s' version is outdated (current: %s, latest: %s)",
            repo_id,
            out_unit.content_type_id,
            *warn_tuple,
        )

    if out_unit.content_type_id == "modulemd_defaults":
        if out_unit.profiles != in_unit.profiles:
            out_unit_name = f"{out_unit.name}:{out_unit.stream}"
            log_warning((out_unit_name, out_unit.profiles, in_unit.profiles))
            return
    if out_unit.content_type_id == "modulemd":
        if out_unit.version < in_unit.version:
            out_unit_name = f"{out_unit.name}:{out_unit.stream}"
            log_warning((out_unit_name, out_unit.version, in_unit.version))
            return
    if out_unit.content_type_id == "rpm":
        out_evr = (out_unit.epoch, out_unit.version, out_unit.release)
        in_evr = (in_unit.epoch, in_unit.version, in_unit.release)
        if RELATION_CMP_MAP["LT"](out_evr, in_evr):  # type: ignore [no-untyped-call]
            log_warning((out_unit.name, out_evr, in_evr))
            return
