from __future__ import annotations

import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from itertools import chain
from typing import Any

from more_executors import Executors
from more_executors.futures import f_proxy
from pubtools.pulplib import RpmDependency, YumRepository

from ubi_manifest.worker.common import get_pkgs_from_all_modules
from ubi_manifest.worker.models import DepsolverItem, PackageToExclude, UbiUnit
from ubi_manifest.worker.pulp_queries import search_rpms
from ubi_manifest.worker.utils import (
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
    """
    Depsolver executes the process of resolving dependencies of binary/debug RPMs
    in the given repositories.
    """

    def __init__(
        self,
        repos: list[DepsolverItem],
        modulemd_dependencies: set[str],
        modular_rpm_filenames: set[str],
        **kwargs: Any,
    ) -> None:
        self.repos: list[DepsolverItem] = repos
        self.modulemd_dependencies: set[str] = modulemd_dependencies
        self.output_set: set[UbiUnit] = set()

        self._provided_rpms: set[RpmDependency] = (
            set()
        )  # set of rpm.provides we've visited
        self._required_rpms: set[RpmDependency] = (
            set()
        )  # set of rpm.requires we've visited

        self._required_files: set[RpmDependency] = (
            set()
        )  # set of files required by visited RPMs
        self._provided_files: set[RpmDependency] = (
            set()
        )  # set of files provided by visited RPMs

        # sets of solvables (pkg, lib, ...) that we use for checking remaining requires
        self._unsolved_rpms: set[RpmDependency] = set()
        self._unsolved_files: set[RpmDependency] = set()

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

    def get_base_packages(
        self,
        repos: list[YumRepository],
        pkgs_list: set[str],
        blacklist: list[PackageToExclude],
    ) -> list[UbiUnit]:
        """
        Query RPMs for given `pkg_list`, returning only latest versions of results.
        """
        crit = create_or_criteria(["name"], [(rpm,) for rpm in pkgs_list])

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
        crit = create_or_criteria(["filename"], [(rpm,) for rpm in pkgs_list])

        content = f_proxy(
            self._executor.submit(search_rpms, crit, repos, BATCH_SIZE_RPM_SPECIFIC)
        )
        return content

    def extract_and_resolve(self, content: set[UbiUnit]) -> None:
        """
        Extracts provides and requires from content and sets internal
        state of self accordingly.
        """
        _required_rpms = set()
        _required_files = set()
        for rpm in content:
            for item in rpm.requires:
                if item.name.startswith("/"):
                    _required_files.add(item)
                elif item.name.startswith("("):
                    # add parsed bool deps to requires that need solving
                    _required_rpms |= parse_bool_deps(item.name)
                else:
                    _required_rpms.add(item)

            for item in rpm.provides:
                # add to global provides
                if item.name.startswith("/"):
                    self._provided_files.add(item)
                else:
                    self._provided_rpms.add(item)

            for filename in rpm.files or []:
                self._provided_files.add(RpmDependency(name=filename))

        # update global requires
        self._required_rpms |= _required_rpms
        self._required_files |= _required_files
        # add new requires to unsolved
        self._unsolved_rpms |= _required_rpms
        self._unsolved_files |= _required_files - self._provided_files

        for prov in self._provided_rpms:
            solved = set()
            for req in self._unsolved_rpms:
                if prov.name != req.name:
                    continue
                if is_requirement_resolved(req, prov):
                    solved.add(req)
            self._unsolved_rpms -= solved

    def what_provides(
        self,
        list_of_requires: list[RpmDependency],
        field: str,
        repos: list[YumRepository],
        blacklist: list[PackageToExclude],
    ) -> list[UbiUnit]:
        """
        Get the latest rpms that provides requirements from list_of_requires in given repos
        """
        # TODO this may pull more than more packages (with different names)
        # for given requirement. It should be decided which one should get into
        # the output. Currently we'll get all matching the query.
        crit = create_or_criteria([field], [(item.name,) for item in list_of_requires])
        content = f_proxy(
            self._executor.submit(search_rpms, crit, repos, BATCH_SIZE_RPM)
        )
        return get_n_latest_from_content(
            content, blacklist, self._modular_rpm_filenames  # type: ignore [arg-type]
        )

    def resolve_files(
        self, repos: list[YumRepository], blacklist: list[PackageToExclude]
    ) -> list[UbiUnit]:
        """
        Resolves file dependencies.
        """
        batch = []
        for _ in range(min(len(self._unsolved_files), BATCH_SIZE_RESOLVER)):
            batch.append(self._unsolved_files.pop())
        return self.what_provides(batch, "files", repos, blacklist)

    def resolve_rpms(
        self, repos: list[YumRepository], blacklist: list[PackageToExclude]
    ) -> list[UbiUnit]:
        """
        Resolves RPM dependencies.
        """
        batch = []
        for _ in range(min(len(self._unsolved_rpms), BATCH_SIZE_RESOLVER)):
            batch.append(self._unsolved_rpms.pop())
        return self.what_provides(batch, "provides.name", repos, blacklist)

    def run(self) -> None:
        """
        Method runs whole depsolving machinery:
        1. Get base packages from each repo input - based on repo whitelist
        2. Until there is nothing left to resolve do:
            A. extract requires and provides from content
            B. set internal state of self accordingly to the content acquired
            C. request new content that provides remaining requirements
            D. content that provides requirements is added to self.output_set
        """
        pulp_repos = list(
            chain.from_iterable([repo.in_pulp_repos for repo in self.repos])
        )

        # Get modular rpms if they are not already populated from the previous run of the depsolver
        if not self._modular_rpm_filenames:
            self._modular_rpm_filenames.update(
                f_proxy(
                    self._executor.submit(
                        get_pkgs_from_all_modules, pulp_repos  # type: ignore [arg-type]
                    )
                )
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

        # wait for base and binary/debug module packages
        for content in as_completed(content_fts):
            self.output_set.update(content.result())

        self._log_missing_base_pkgs()

        to_resolve = set(self.output_set)
        while not self._base_pkgs_only:
            # extract provides and requires
            self.extract_and_resolve(to_resolve)
            # we are finished if _unsolved_rpms/files are empty
            if not self._unsolved_rpms and not self._unsolved_files:
                break
            # get new content that provides required RPMs and files
            resolved = self.resolve_rpms(pulp_repos, merged_blacklist)
            resolved.extend(self.resolve_files(pulp_repos, merged_blacklist))
            # add content to the output set
            self.output_set.update(resolved)
            # new content needs resolving
            to_resolve = set(resolved)

        if not self._base_pkgs_only:
            # log warnings if depsolving failed
            deps_not_found = {req.name for req in self._required_rpms} - {
                prov.name for prov in self._provided_rpms
            }
            deps_not_found |= {req.name for req in self._required_files} - {
                prov.name for prov in self._provided_files
            }
            if deps_not_found:
                self._log_warnings(deps_not_found, pulp_repos, merged_blacklist)

    def export(self) -> dict[str, list[UbiUnit]]:
        """
        Prepares output, deduplicating units while keeping identical RPMs from
        different repositories.
        """
        out: dict[str, list[UbiUnit]] = {}
        # set of unique tuples (filename, repo_id)
        filename_repo_tuples: set[tuple[str, str]] = set()
        for item in self.output_set:
            # deduplicate output sets, but keep identical rpms that have different repository
            # we can't easily decide which one we should keep/discard.
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
                # This is expected, so logging is only at the info level
                _LOG.info(
                    "Failed depsolving: %s is blacklisted. These rpms depend on it %s",
                    item,
                    sorted(depending_rpms),
                )
            else:
                _LOG.warning(
                    "Failed depsolving: %s can not be found in these input repos: %s. "
                    "These rpms depend on it %s",
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
