from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import chain
from typing import Any

from more_executors import Executors
from pubtools.pulplib import YumRepository

from ubi_manifest.worker.models import PackageToExclude, UbiUnit
from ubi_manifest.worker.pulp_queries import search_rpms
from ubi_manifest.worker.utils import (
    create_or_criteria,
    is_blacklisted,
)

MAX_WORKERS = int(os.getenv("UBI_MANIFEST_SRPM_DEPSOLVER_WORKERS", "8"))
BATCH_SIZE_SRPM_SPECIFIC = int(
    os.getenv("UBI_MANIFEST_BATCH_SIZE_SRPM_SPECIFIC", "500")
)


class SrpmDepsolver:
    """
    SRPM Depsolver class performs search for given source rpms (SRPMs).
    """

    def __init__(
        self,
        srpm_filenames: dict[str, set[str]],
        input_source_repos: list[YumRepository],
        srpm_blacklists: list[list[PackageToExclude]],
    ) -> None:
        self.srpm_filenames = srpm_filenames
        self.input_source_repos = input_source_repos

        self.merged_blacklist: list[PackageToExclude] = list(
            chain.from_iterable(srpm_blacklists)
        )
        self.srpm_output_set: set[UbiUnit] = set()

        self._executor: ThreadPoolExecutor = Executors.thread_pool(  # type: ignore [assignment]
            max_workers=MAX_WORKERS
        )

    def __enter__(self) -> SrpmDepsolver:
        return self

    def __exit__(self, *args: Any, **kwargs: Any) -> None:
        self._executor.__exit__(*args, **kwargs)

    def run(self) -> None:
        """
        This method iterates through source repos and searches for SRPMs
        listed in srpm_filenames under the current source repo id as key.
        """
        content_fts = []
        for srpm_repo in self.input_source_repos:
            if not self.srpm_filenames.get(srpm_repo.id):
                continue
            rpms_to_search = self.srpm_filenames[srpm_repo.id]
            crit = create_or_criteria(["filename"], [(rpm,) for rpm in rpms_to_search])
            content_fts.append(
                self._executor.submit(
                    search_rpms, crit, [srpm_repo], BATCH_SIZE_SRPM_SPECIFIC
                )
            )
        for content_ft in as_completed(content_fts):
            self.srpm_output_set.update(
                {
                    srpm
                    for srpm in content_ft.result()  # type: ignore [attr-defined]
                    if not is_blacklisted(srpm, self.merged_blacklist)
                }
            )

    def export(self) -> dict[str, list[UbiUnit]]:
        """
        Prepares output.
        """
        out: dict[str, list[UbiUnit]] = {}

        for item in self.srpm_output_set:
            out.setdefault(item.associate_source_repo_id, []).append(item)

        return out
