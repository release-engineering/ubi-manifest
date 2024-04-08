from __future__ import annotations

import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from itertools import chain
from typing import Any

from more_executors import Executors
from more_executors.futures import f_proxy
from pubtools.pulplib import Criteria, RpmDependency, YumRepository

from ubi_manifest.worker.tasks.depsolver.models import PackageToExclude

from .models import DepsolverItem, UbiUnit
from .pulp_queries import search_modulemds, search_rpms
from .utils import (
    _is_blacklisted,
    create_or_criteria,
    get_n_latest_from_content,
    is_requirement_resolved,
    parse_bool_deps,
)

_LOG = logging.getLogger(__name__)

# need to set significantly lower batches for general rpm search
# otherwise db may very likely hit OOM error.
BATCH_SIZE_RPM = int(os.getenv("UBI_MANIFEST_BATCH_SIZE_RPM", "25"))
# limit for batch of specific search of rpms (e.g. via filename)
BATCH_SIZE_RPM_SPECIFIC = int(os.getenv("UBI_MANIFEST_BATCH_SIZE_RPM_SPECIFIC", "500"))
BATCH_SIZE_RESOLVER = int(os.getenv("UBI_MANIFEST_BATCH_SIZE_RESOLVER", "150"))
MAX_WORKERS = int(os.getenv("UBI_MANIFEST_DEPSOLVER_WORKERS", "8"))


class Depsolver:
    def __init__(
        self,
        repos: list[DepsolverItem],
        srpm_repos: list[Future[YumRepository]],
        modulemd_dependencies: set[str],
        modular_rpm_filenames: set[str],
        **kwargs: Any,
    ) -> None:
        self.repos: list[DepsolverItem] = repos
        self.modulemd_dependencies: set[str] = modulemd_dependencies
        self.output_set: set[UbiUnit] = set()
        self.srpm_output_set: set[UbiUnit] = set()

        self._srpm_repos: list[Future[YumRepository]] = srpm_repos

        self._provides: set[RpmDependency] = set()  # set of rpm.provides we've visited
        self._requires: set[RpmDependency] = set()  # set of rpm.requires we've visited

        # set of solvables (pkg, lib, ...) that we use for checking remaining requires
        self._unsolved: set[RpmDependency] = set()

        # Set of all modular rpms. Modifying the given modular_rpm_filenames set in place
        self._modular_rpm_filenames: set[str] = modular_rpm_filenames

        self._executor: ThreadPoolExecutor = Executors.thread_pool(  # type: ignore [assignment]
            max_workers=MAX_WORKERS
        )
        self._base_pkgs_only = kwargs.get("base_pkgs_only") or False

    def __enter__(self) -> Depsolver:
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self._executor.__exit__(*args, **kwargs)

    def _get_pkgs_from_all_modules(
        self, repos: list[YumRepository]
    ) -> Future[set[str]]:
        """
        Search for modulemds in all input repos and extract rpm filenames.
        """

        def extract_modular_filenames() -> set[str]:
            filenames = set()
            for module in modules:  # type: ignore [attr-defined]
                filenames |= set(module.artifacts_filenames)

            return filenames

        modules = search_modulemds([Criteria.true()], repos)
        return f_proxy(self._executor.submit(extract_modular_filenames))

    def get_base_packages(
        self,
        repos: list[YumRepository],
        pkgs_list: set[str],
        blacklist: list[PackageToExclude],
    ) -> list[UbiUnit]:
        crit = create_or_criteria(["name"], [(name,) for name in pkgs_list])

        content = f_proxy(
            self._executor.submit(search_rpms, crit, repos, BATCH_SIZE_RPM)
        )

        newest_rpms = get_n_latest_from_content(
            content, blacklist, self._modular_rpm_filenames  # type: ignore [arg-type]
        )

        return newest_rpms

    def get_modulemd_packages(
        self, repos: list[YumRepository], pkgs_list: set[str]
    ) -> Future[Future[set[UbiUnit]]]:
        """
        Search for modular rpms.
        """
        crit = create_or_criteria(["filename"], [(name,) for name in pkgs_list])

        content = f_proxy(
            self._executor.submit(search_rpms, crit, repos, BATCH_SIZE_RPM_SPECIFIC)
        )
        return content

    def extract_and_resolve(self, content: set[UbiUnit]) -> None:
        """
        Extracts provides and requires from content and sets internal
        state of self accordingly.
        """
        _requires = set()
        _file_reqs = set()
        _has_files = set()
        for rpm in content:
            if rpm.files:
                # collect to potentially save iterations later
                _has_files.add(rpm)
            for item in rpm.requires:
                if item.name.startswith("/"):
                    _file_reqs.add(item)
                elif item.name.startswith("("):
                    # add parsed bool deps to requires that need solving
                    _requires |= parse_bool_deps(item.name)
                else:
                    _requires.add(item)

            for item in rpm.provides:
                # add to global provides
                self._provides.add(item)

        # add rpm dependencies for file requirements
        for item in _file_reqs:
            for rpm in _has_files:
                if item.name in rpm.files:
                    _requires.add(RpmDependency(name=rpm.name))

        # update global requires
        self._requires |= _requires
        # add new requires to unsolved
        self._unsolved |= _requires

        for prov in self._provides:
            solved = set()
            for req in self._unsolved:
                if prov.name != req.name:
                    continue
                if is_requirement_resolved(req, prov):
                    solved.add(req)
            self._unsolved -= solved

    def what_provides(
        self,
        list_of_requires: list[RpmDependency],
        repos: list[YumRepository],
        blacklist: list[PackageToExclude],
    ) -> list[UbiUnit]:
        """
        Get the latest rpms that provides requirements from list_of_requires in given repos
        """
        # TODO this may pull more than more packages (with different names)
        # for given requirement. It should be decided which one should get into
        # the output. Currently we'll get all matching the query.
        crit = create_or_criteria(
            ["provides.name"], [(item.name,) for item in list_of_requires]
        )

        content = f_proxy(
            self._executor.submit(search_rpms, crit, repos, BATCH_SIZE_RPM)
        )
        newest_rpms = get_n_latest_from_content(
            content, blacklist, self._modular_rpm_filenames  # type: ignore [arg-type]
        )

        return newest_rpms

    def get_source_pkgs(
        self, binary_rpms: list[UbiUnit], blacklist: list[PackageToExclude]
    ) -> set[UbiUnit]:
        crit = create_or_criteria(
            ["filename"], [(rpm.sourcerpm,) for rpm in binary_rpms if rpm.sourcerpm]
        )

        content = f_proxy(
            self._executor.submit(
                search_rpms, crit, self._srpm_repos, BATCH_SIZE_RPM_SPECIFIC
            )
        )

        return {rpm for rpm in content if not _is_blacklisted(rpm, blacklist)}  # type: ignore [attr-defined]

    def run(self) -> None:
        """
        Method runs whole depsolving machinery:
        1. Get base packages from each repo input - based on repo whitelist
        2. Until there is nothing left to resolve do:
            A. extract requires and provides from content
            B. set internal state of self accordingly to the content acquired
            C. request new content that provides remaining requirements
            D. content that provides requirements is added to self.output_set
        3. During phase 1. and 2. source RPM packages are queried for already acquired RPMS.
        """
        pulp_repos = list(
            chain.from_iterable([repo.in_pulp_repos for repo in self.repos])
        )

        # Get modular rpms if they are not already populated from the previous run of the depsolver
        if not self._modular_rpm_filenames:
            self._modular_rpm_filenames.update(
                self._get_pkgs_from_all_modules(pulp_repos)  # type: ignore [arg-type]
            )

        merged_blacklist = list(
            chain.from_iterable([repo.blacklist for repo in self.repos])
        )

        # search for base rpms
        content_fts = [
            self._executor.submit(
                self.get_base_packages,
                repo.in_pulp_repos,
                repo.whitelist,
                repo.blacklist,
            )
            for repo in self.repos
        ]

        # Get modulemd binary/debug rpm dependencies
        if self.modulemd_dependencies:
            content_fts.append(
                self._executor.submit(
                    self.get_modulemd_packages,  # type: ignore [arg-type]
                    pulp_repos,
                    self.modulemd_dependencies,
                )
            )

        # Get source rpms of found base packages and binary/debug modular packages
        source_rpm_fts = []
        for content in as_completed(content_fts):
            self.output_set.update(content.result())
            ft = self._executor.submit(
                self.get_source_pkgs, content.result(), merged_blacklist
            )
            source_rpm_fts.append(ft)

        self._log_missing_base_pkgs()

        to_resolve = set(self.output_set)
        while True and not self._base_pkgs_only:
            # extract provides and requires
            self.extract_and_resolve(to_resolve)
            # we are finished if _unsolved is empty
            if not self._unsolved:
                break

            batch = []
            # making batch as the query for provides.name in rpm units is slow in general
            # we'll better do it in smaller batches
            for _ in range(self._batch_size()):
                batch.append(self._unsolved.pop())
            # get new content that provides current batch of requires
            resolved = self.what_provides(batch, pulp_repos, merged_blacklist)
            # new content needs resolving deps
            to_resolve = set(resolved)
            # add content to the output set
            self.output_set.update(resolved)
            # submit query for source rpms
            ft = self._executor.submit(self.get_source_pkgs, resolved, merged_blacklist)
            source_rpm_fts.append(ft)

        # wait for srpm queries and store them in the output set
        for srpm_content in as_completed(source_rpm_fts):
            for srpm in srpm_content.result():
                self.srpm_output_set.add(srpm)

        if not self._base_pkgs_only:
            # log warnings if depsolving failed
            deps_not_found = {req.name for req in self._requires} - {
                prov.name for prov in self._provides
            }
            if deps_not_found:
                self._log_warnings(deps_not_found, pulp_repos, merged_blacklist)

    def _batch_size(self) -> int:
        if len(self._unsolved) < BATCH_SIZE_RESOLVER:
            batch_size = len(self._unsolved)
        else:
            batch_size = BATCH_SIZE_RESOLVER

        return batch_size

    def export(self) -> dict[str, list[UbiUnit]]:
        out: dict[str, list[UbiUnit]] = {}
        # set of unique tuples (filename, repo_id)
        filename_repo_tuples: set[tuple[str, str]] = set()
        for item in self.output_set | self.srpm_output_set:
            # deduplicate output sets, but keep identical rpms that have different repository
            # we can't easily decide which one we should keep/discard.
            # one SRPM can be shared with more than one binary/debug RPM
            # one debug RPM may be related with more binary RPMs
            if (
                item.filename,
                item.associate_source_repo_id,
            ) not in filename_repo_tuples:
                filename_repo_tuples.add((item.filename, item.associate_source_repo_id))
                out.setdefault(item.associate_source_repo_id, []).append(item)

        return out

    def _log_warnings(
        self,
        deps_not_found: set[str],
        pulp_repos: list[YumRepository],
        merged_blacklist: list[PackageToExclude],
    ) -> None:
        """
        Log failed depsolving. We print out the rpms whose direct dependencies
        could not be included in output set.
        """
        input_repos = [x.id for x in pulp_repos]

        # To determine if dep is missing due to being blacklisted
        def _is_blacklisted_by_rule(item: str, rule: PackageToExclude) -> bool:
            if rule.globbing:
                return item.startswith(rule.name)
            return item == rule.name

        def _requires_names(requires: list[RpmDependency]) -> set[str]:
            out = set()
            for item in requires:
                if item.name.startswith("("):
                    out |= {dep.name for dep in parse_bool_deps(item.name)}
                else:
                    out.add(item.name)
            return out

        # Get rpms depending on missing dependencies
        for item in deps_not_found:
            depending_rpms = list(
                rpm.filename
                for rpm in self.output_set
                if item in _requires_names(rpm.requires)
            )

            # Divide missing dependencies blacklisted and all others
            if any((_is_blacklisted_by_rule(item, rule) for rule in merged_blacklist)):
                _LOG.warning(
                    "Failed depsolving: %s is blacklisted. These rpms depend on it %s",
                    item,
                    sorted(depending_rpms),
                )
            else:
                _LOG.warning(
                    "Failed depsolving: %s can not be found in these input repos: %s. These rpms depend on it %s",
                    item,
                    input_repos,
                    sorted(depending_rpms),
                )

    def _log_missing_base_pkgs(self) -> None:
        found_pkg_names = {item.name for item in self.output_set}

        for item in self.repos:
            missing = item.whitelist - found_pkg_names
            if missing:
                repos = [repo.id for repo in item.in_pulp_repos]
                for pkg_name in missing:
                    _LOG.warning("'%s' not found in %s.", pkg_name, repos)
