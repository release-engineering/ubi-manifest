import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain
from typing import List, Set

from more_executors import Executors
from more_executors.futures import f_proxy
from pubtools.pulplib import Criteria

from .models import DepsolverItem
from .pulp_queries import search_modulemds, search_rpms
from .utils import create_or_criteria, get_n_latest_from_content, parse_bool_deps

_LOG = logging.getLogger(__name__)

# need to set significantly lower batches for general rpm search
# otherwise db may very likely hit OOM error.
BATCH_SIZE_RPM = int(os.getenv("UBI_MANIFEST_BATCH_SIZE_RPM", "15"))
BATCH_SIZE_RESOLVER = int(os.getenv("UBI_MANIFEST_BATCH_SIZE_RESOLVER", "150"))


class Depsolver:
    def __init__(self, repos: List[DepsolverItem]) -> None:

        self.repos: List[DepsolverItem] = repos
        self.output_set: Set = set()

        self._provides: Set = set()  # set of all rpm.provides we've visited
        self._requires: Set = set()  # set of all rpm.requires we've visited

        # set of solvables (pkg, lib, ...) that we use for checking remaining requires
        self._unsolved: Set = set()

        self._modular_rpms: Set = set()

        self._executor: ThreadPoolExecutor = Executors.thread_pool(max_workers=4)

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

    def get_base_packages(self, repos, pkgs_list):
        crit = create_or_criteria(["name"], [(rpm,) for rpm in pkgs_list])

        content = f_proxy(
            self._executor.submit(search_rpms, crit, repos, BATCH_SIZE_RPM)
        )

        newest_rpms = get_n_latest_from_content(content, self._modular_rpms)
        return newest_rpms

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
                    _requires.add(item.name)
            for item in rpm.provides:
                # add to global provides
                self._provides.add(item.name)

        # update global requires
        self._requires |= _requires
        # add new requires to unsolved
        self._unsolved |= _requires
        # get solved requires
        solved = self._unsolved & self._provides
        # and subtract solved requires
        self._unsolved -= solved

    def what_provides(self, list_of_requires, repos):
        """
        Get the latest rpms that provides requirements from list_of_requires in given repos
        """
        crit = create_or_criteria(
            ["provides.name"], [(item,) for item in list_of_requires]
        )

        content = f_proxy(
            self._executor.submit(search_rpms, crit, repos, BATCH_SIZE_RPM)
        )
        newest_rpms = get_n_latest_from_content(content, self._modular_rpms)

        return newest_rpms

    def run(self):
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
        # get modular rpms first
        self._modular_rpms = self._get_pkgs_from_all_modules(pulp_repos)

        # search for rpms
        content_fts = [
            self._executor.submit(
                self.get_base_packages, repo.in_pulp_repos, repo.whitelist
            )
            for repo in self.repos
        ]
        for content in as_completed(content_fts):
            self.output_set.update(content.result())

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
            # get new content that provides current batch of rqeuires
            resolved = self.what_provides(batch, pulp_repos)
            # new content needs resolving deps
            to_resolve = set(resolved)
            # add contetnt to the output set
            self.output_set.update(resolved)

    def _batch_size(self):
        if len(self._unsolved) < BATCH_SIZE_RESOLVER:
            batch_size = len(self._unsolved)
        else:
            batch_size = BATCH_SIZE_RESOLVER

        return batch_size

    def export(self):
        out = {}
        for item in self.output_set:
            out.setdefault(item.associate_source_repo_id, []).append(item)

        return out
