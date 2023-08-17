import logging
import os
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from itertools import chain
from typing import List, Set

from more_executors import Executors
from more_executors.futures import f_proxy
from pubtools.pulplib import Criteria, YumRepository

from .models import DepsolverItem, UbiUnit
from .pulp_queries import search_modulemds, search_rpms
from .utils import (
    _is_blacklisted,
    create_or_criteria,
    get_n_latest_from_content,
    parse_bool_deps,
    is_requirement_resolved,
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
        repos: List[DepsolverItem],
        srpm_repos,
        modulemd_dependencies: Set[str],
    ) -> None:
        self.repos: List[DepsolverItem] = repos
        self.modulemd_dependencies: Set[str] = modulemd_dependencies
        self.output_set: Set[UbiUnit] = set()
        self.srpm_output_set: Set[UbiUnit] = set()

        self._srpm_repos: List[Future[YumRepository]] = srpm_repos

        self._provides: Set = set()  # set of all rpm.provides we've visited
        self._requires: Set = set()  # set of all rpm.requires we've visited

        # set of solvables (pkg, lib, ...) that we use for checking remaining requires
        self._unsolved: Set = set()

        # Set of all modular rpms
        self._modular_rpms: Set = set()

        self._executor: ThreadPoolExecutor = Executors.thread_pool(
            max_workers=MAX_WORKERS
        )

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self._executor.__exit__(*args, **kwargs)

    def _get_pkgs_from_all_modules(self, repos):
        # search for modulemds in all input repos
        # and extract filenames only
        def extract_modular_filenames():
            modular_rpm_filenames = set()
            for module in modules:
                modular_rpm_filenames |= set(module.artifacts_filenames)

            return modular_rpm_filenames

        modules = search_modulemds([Criteria.true()], repos)
        return f_proxy(self._executor.submit(extract_modular_filenames))

    def get_base_packages(self, repos, pkgs_list, blacklist):
        crit = create_or_criteria(["name"], [(rpm,) for rpm in pkgs_list])

        content = f_proxy(
            self._executor.submit(search_rpms, crit, repos, BATCH_SIZE_RPM)
        )
        newest_rpms = get_n_latest_from_content(content, blacklist, self._modular_rpms)
        return newest_rpms

    def get_modulemd_packages(self, repos, pkgs_list):
        """
        Search for modulemd dependencies
        """
        crit = create_or_criteria(["filename"], [(rpm,) for rpm in pkgs_list])

        content = f_proxy(
            self._executor.submit(search_rpms, crit, repos, BATCH_SIZE_RPM_SPECIFIC)
        )
        return content

    def extract_and_resolve(self, content):
        """
        Extracts provides and requires from content and sets internal
        state of self accordingly.
        """
        _requires = set()
        for rpm in content:
            for item in rpm.requires:
                # skip scriplet requires
                if item.name.startswith("/"):
                    continue
                if item.name.startswith("("):
                    # add parsed bool deps to requires that need solving
                    _requires |= parse_bool_deps(item.name)
                else:
                    _requires.add(item)

            for item in rpm.provides:
                # add to global provides
                self._provides.add(item)

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

    def what_provides(self, list_of_requires, repos, blacklist):
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
        newest_rpms = get_n_latest_from_content(content, blacklist, self._modular_rpms)

        return newest_rpms

    def get_source_pkgs(self, binary_rpms, blacklist):
        crit = create_or_criteria(
            ["filename"], [(rpm.sourcerpm,) for rpm in binary_rpms if rpm.sourcerpm]
        )

        content = f_proxy(
            self._executor.submit(
                search_rpms, crit, self._srpm_repos, BATCH_SIZE_RPM_SPECIFIC
            )
        )

        return {rpm for rpm in content if not _is_blacklisted(rpm, blacklist)}

    def run(self):
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
        # get modular rpms first
        self._modular_rpms = self._get_pkgs_from_all_modules(pulp_repos)

        merged_blacklist = list(
            chain.from_iterable([repo.blacklist for repo in self.repos])
        )
        # search for rpms
        content_fts = [
            self._executor.submit(
                self.get_base_packages,
                repo.in_pulp_repos,
                repo.whitelist,
                repo.blacklist,
            )
            for repo in self.repos
        ]

        source_rpm_fts = []

        if self.modulemd_dependencies:
            # Divide modular dependencies into binary and modular rpms
            binary_modular_deps = set()
            source_modular_deps = set()
            for item in self.modulemd_dependencies:
                if ".src.rpm" in item:
                    source_modular_deps.add(item)
                else:
                    binary_modular_deps.add(item)

            # Get moduelmd binary rpm dependencies
            if binary_modular_deps:
                content_fts.append(
                    self._executor.submit(
                        self.get_modulemd_packages, pulp_repos, binary_modular_deps
                    )
                )

            # Get modulemd source rpm dependencies
            if source_modular_deps:
                source_rpm_fts.append(
                    self._executor.submit(
                        self.get_modulemd_packages,
                        self._srpm_repos,
                        source_modular_deps,
                    )
                )

        for content in as_completed(content_fts):
            self.output_set.update(content.result())
            ft = self._executor.submit(
                self.get_source_pkgs, content.result(), merged_blacklist
            )
            source_rpm_fts.append(ft)

        to_resolve = set(self.output_set)
        while True:
            # extract provides and requires
            self.extract_and_resolve(to_resolve)
            # we are finished if _ensolved is empty
            if not self._unsolved:
                break

            batch = []
            # making batch as the query for provides.name in rpm units is slow in general
            # we'll better do it is smaller batches
            for _ in range(self._batch_size()):
                batch.append(self._unsolved.pop())
            # get new content that provides current batch of requires
            resolved = self.what_provides(batch, pulp_repos, merged_blacklist)
            # new content needs resolving deps
            to_resolve = set(resolved)
            # add contetnt to the output set
            self.output_set.update(resolved)
            # submit query for source rpms
            ft = self._executor.submit(self.get_source_pkgs, resolved, merged_blacklist)
            source_rpm_fts.append(ft)

        # wait for srpm queries and store them the output set
        for srpm_content in as_completed(source_rpm_fts):
            for srpm in srpm_content.result():
                self.srpm_output_set.add(srpm)

        # log warnings if depsolving failed
        deps_not_found = {req.name for req in self._requires} - {
            prov.name for prov in self._provides
        }
        if deps_not_found:
            self._log_warnings(deps_not_found, pulp_repos, merged_blacklist)

    def _batch_size(self):
        if len(self._unsolved) < BATCH_SIZE_RESOLVER:
            batch_size = len(self._unsolved)
        else:
            batch_size = BATCH_SIZE_RESOLVER

        return batch_size

    def export(self):
        out = {}
        # set of unique tuples (filename, repo_id)
        filename_repo_tuples = set()
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

    def _log_warnings(self, deps_not_found, pulp_repos, merged_blacklist):
        """
        Log failed depsolving. We print out the rpms whose direct dependencies
        could not be included in output set.
        """
        input_repos = [x.id for x in pulp_repos]

        # To determine if dep is missing due to being blacklisted
        def _is_blacklisted_by_rule(item, rule):
            if rule.globbing:
                return item.startswith(rule.name)
            return item == rule.name

        def _requires_names(requires):
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
