from pubtools.pulplib import Criteria
import os
import logging
from concurrent.futures import as_completed
from pubtools.pulplib import Criteria
from more_executors.futures import f_proxy
from more_executors import Executors
from .pulp_queries import search_modulemds, search_rpms
from .utils import _create_or_criteria, get_n_latest_from_content, parse_bool_deps

_LOG = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("UBIPOP_BATCH_SIZE", "250"))  ###do konfigu
# need to set significantly lower batches for general rpm search
# otherwise db may very likely hit OOM error.
BATCH_SIZE_RPM = int(os.getenv("UBIPOP_BATCH_SIZE_RPM", "15"))  ###do konfigu
BATCH_SIZE_RESOLVER = int(os.getenv("UBIPOP_BATCH_SIZE_RESOLVER", "150"))  ###do konfigu


class Depsolver:
    def __init__(self, repos):

        self.repos = repos
        self.output_set = set()

        self._provides = set()
        self._requires = set()
        self._unsolved = set()
        self._modular_rpms = set()

        self._executor = Executors.thread_pool(max_workers=4)

    def _get_pkgs_from_all_modules(self, repo):
        # search for modulesmds in all input repos
        # and extract filenames only
        def extract_modular_filenames():
            modular_rpm_filenames = set()
            for module in modules:
                modular_rpm_filenames |= set(module.artifacts_filenames)

            return modular_rpm_filenames

        modules = search_modulemds([Criteria.true()], repo)
        return f_proxy(self._executor.submit(extract_modular_filenames))

    def get_base_packages(self, repo, pkgs_list):
        crit = _create_or_criteria(["name"], [(rpm,) for rpm in pkgs_list])

        content = f_proxy(
            self._executor.submit(search_rpms, crit, [repo], BATCH_SIZE_RPM)
        )
        ####to udelat na zacatku, get from multiple repos
        # (to prece umim)
        newest_rpms = get_n_latest_from_content(content, self._modular_rpms)
        ### extract requires and provides
        return newest_rpms

    def extract_and_resolve(self, content):
        _requires = set()
        for rpm in content:
            for item in rpm.requires:
                if item.name.startswith("("):
                    # add parsed bool deps to requires that need solving
                    _requires |= parse_bool_deps(item.name)
                else:
                    _requires.add(item.name)
            for item in rpm.provides:
                # add to global provides
                self._provides.add(item.name)

        # update globab requires
        self._requires |= _requires
        # add new requires to unsolved
        self._unsolved |= _requires
        # get solved rqeuires
        solved = self._unsolved & self._provides
        # and subtract solved requires (also previously solved)
        self._unsolved -= solved

    def what_provides(self, list_of_requires, repos):
        crit = _create_or_criteria(
            ["provides.name"], [(item,) for item in list_of_requires]
        )

        content = f_proxy(
            self._executor.submit(search_rpms, crit, repos, BATCH_SIZE_RPM)
        )
        newest_rpms = get_n_latest_from_content(content, self._modular_rpms)

        return newest_rpms

    def run(self):
        pulp_repos = [repo.in_pulp_repo for repo in self.repos]
        # get modular rpms first
        self._modular_rpms = self._get_pkgs_from_all_modules(pulp_repos)

        # search for rpms
        content_fts = [
            self._executor.submit(
                self.get_base_packages, repo.in_pulp_repo, repo.whitelist
            )
            for repo in self.repos
        ]

        for content in as_completed(content_fts):
            self.output_set.update(content.result())

        to_resolve = set(self.output_set)

        while True:
            self.extract_and_resolve(to_resolve)
            if not self._unsolved:
                break

            batch = []
            if len(self._unsolved) < BATCH_SIZE_RESOLVER:
                BATCH = len(self._unsolved)
            else:
                BATCH = BATCH_SIZE_RESOLVER

            # making batch as the query for provides.name in rpm units is slow in general
            # we'll better do it is smaller batches
            for _ in range(BATCH):
                batch.append(self._unsolved.pop())
            ### get new content that provides current batch of rqeuires
            resolved = self.what_provides(batch, pulp_repos)
            self.output_set.update(resolved)
