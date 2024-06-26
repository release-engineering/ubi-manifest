import logging
from collections import defaultdict
from concurrent.futures import Future, as_completed
from typing import Any

from more_executors.futures import f_proxy
from pubtools.pulplib import Criteria, ModulemdDefaultsUnit, ModulemdUnit, RpmUnit

from ubi_manifest.worker.tasks.celery import app
from ubi_manifest.worker.tasks.depsolve import filter_whitelist, get_content_config
from ubi_manifest.worker.tasks.depsolver.models import PackageToExclude, UbiUnit
from ubi_manifest.worker.tasks.depsolver.pulp_queries import search_units
from ubi_manifest.worker.tasks.depsolver.ubi_config import UbiConfigLoader
from ubi_manifest.worker.tasks.depsolver.utils import (
    RELATION_CMP_MAP,
    create_or_criteria,
    get_criteria_for_modules,
    is_blacklisted,
    keep_n_latest_modulemd_defaults,
    keep_n_latest_modules,
    keep_n_latest_rpms,
    make_pulp_client,
    parse_blacklist_config,
)

_LOG = logging.getLogger(__name__)

RPM_FIELDS = ["name", "version", "release", "arch"]
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
            # get all relevant units currently on output repo
            out_rpms = f_proxy(
                search_units(
                    out_repo, [Criteria.true()], RpmUnit, unit_fields=RPM_FIELDS
                )
            )
            out_mds = f_proxy(
                search_units(
                    out_repo, [Criteria.true()], ModulemdUnit, unit_fields=MD_FIELDS
                )
            )
            out_mdds = f_proxy(
                search_units(out_repo, [Criteria.true()], ModulemdDefaultsUnit)
            )

            seen_units: set[UbiUnit] = set()
            output_whitelist: set[str] = set()
            output_blacklist: list[PackageToExclude] = []

            in_rpms_fts: list[Future[set[UbiUnit]]] = []
            in_mds_fts: list[Future[set[UbiUnit]]] = []
            in_mdds_fts: list[Future[set[UbiUnit]]] = []

            for in_repo in client.search_repository(
                Criteria.with_id(out_repo.population_sources)
            ):
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
                    whitelist, debuginfo_whitelist = filter_whitelist(config)
                    output_whitelist |= whitelist | debuginfo_whitelist
                    output_blacklist.extend(parse_blacklist_config(config))

            # check that all content is up-to-date
            out_rpms_result = out_rpms.result()
            for in_rpm in _latest_input_rpms(in_rpms_fts):
                for out_rpm in out_rpms_result.copy():
                    if (out_rpm.name, out_rpm.arch) == (in_rpm.name, in_rpm.arch):
                        _compare_versions(out_repo.id, out_rpm, in_rpm)
                        seen_units.add(in_rpm)
                        out_rpms_result.discard(out_rpm)
                        break
            out_mds_result = out_mds.result()
            for in_md in _latest_input_mds(in_mds_fts):
                for out_md in out_mds_result.copy():
                    if (out_md.name, out_md.stream) == (in_md.name, in_md.stream):
                        _compare_versions(out_repo.id, out_md, in_md)
                        seen_units.add(in_md)
                        out_mds_result.discard(out_md)
                        break
            out_mdds_result = out_mdds.result()
            for in_mdd in _latest_input_mdds(in_mdds_fts):
                for out_mdd in out_mdds_result.copy():
                    if out_mdd.name == in_mdd.name:
                        _compare_versions(out_repo.id, out_mdd, in_mdd)
                        out_mdds_result.discard(out_mdd)
                        break

            # check seen units against blacklist
            if blacklisted := {
                u.name for u in seen_units if is_blacklisted(u, output_blacklist)
            }:
                _LOG.warning(
                    "[%s] blacklisted content found in input repositories;\n\t%s",
                    out_repo.id,
                    "\n\t".join(sorted(blacklisted)),
                )

            # check seen units off of whitelist
            for pattern in output_whitelist.copy():
                if [u.name for u in seen_units if pattern in u.name]:
                    output_whitelist.remove(pattern)

            # report any missing whitelisted packages for the output repo
            if output_whitelist:
                _LOG.warning(
                    "[%s] whitelisted content not found in population source repositories;\n\t%s",
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
    out = []
    for ft in as_completed(fts):
        out.extend(list(ft.result()))
    keep_n_latest_modules(out)
    return out


def _latest_input_mdds(fts: list[Future[set[UbiUnit]]]) -> list[UbiUnit]:
    out = []
    for ft in as_completed(fts):
        out.extend(list(ft.result()))
    keep_n_latest_modulemd_defaults(out)
    return out


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
