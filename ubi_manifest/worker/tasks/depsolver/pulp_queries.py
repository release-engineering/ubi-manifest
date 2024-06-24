import os
from concurrent.futures import Future
from typing import Optional

from more_executors.futures import f_flat_map, f_map, f_proxy, f_return, f_sequence
from pubtools.pulplib import (
    Criteria,
    ModulemdDefaultsUnit,
    ModulemdUnit,
    Page,
    RpmUnit,
    Unit,
    YumRepository,
)

from .models import UbiUnit
from .utils import flatten_list_of_sets

BATCH_SIZE = int(os.getenv("UBI_MANIFEST_BATCH_SIZE", "250"))

RPM_FIELDS = ["name", "filename", "sourcerpm", "requires", "provides", "files"]
MODULEMD_FIELDS = [
    "name",
    "stream",
    "version",
    "context",
    "arch",
    "dependencies",
    "profiles",
    "artifacts",
]

UNIT_FIELDS = {
    RpmUnit: RPM_FIELDS,
    ModulemdUnit: MODULEMD_FIELDS,
    # using fields limit to query doesn't work for modulemd_defaults unit
}


def search_units(
    repo: YumRepository,
    criteria_list: list[Criteria],
    content_type_cls: Unit,
    batch_size_override: Optional[int] = None,
    unit_fields: Optional[list[str]] = None,
) -> Future[set[UbiUnit]]:
    """
    Search for units of one content type associated with given repository by criteria.
    """
    units = set()
    batch_size = batch_size_override or BATCH_SIZE
    unit_fields = unit_fields or UNIT_FIELDS.get(content_type_cls, None)

    def handle_results(page: Page) -> Future[set[UbiUnit]]:
        for unit in page.data:
            unit = UbiUnit(unit, repo.id)
            units.add(unit)
        if page.next:
            return f_flat_map(page.next, handle_results)
        return f_return(units)

    criteria_split = []

    for start in range(0, len(criteria_list), batch_size):
        criteria_split.append(criteria_list[start : start + batch_size])
    fts = []

    for criteria_batch in criteria_split:
        _criteria = Criteria.and_(
            Criteria.with_unit_type(content_type_cls, unit_fields=unit_fields),
            Criteria.or_(*criteria_batch),
        )

        page_f = repo.search_content(_criteria)
        handled_f = f_flat_map(page_f, handle_results)

        fts.append(handled_f)

    return f_map(f_sequence(fts), flatten_list_of_sets)


def _search_units_per_repos(
    or_criteria: list[Criteria],
    repos: list[YumRepository],
    content_type_cls: Unit,
    batch_size_override: Optional[int] = None,
) -> Future[set[UbiUnit]]:
    units = []
    for repo in repos:
        units.append(
            search_units(
                repo,
                or_criteria,
                content_type_cls,
                batch_size_override=batch_size_override,
            )
        )

    return f_proxy(f_map(f_sequence(units), flatten_list_of_sets))


def search_modulemds(
    or_criteria: list[Criteria],
    repos: list[YumRepository],
    batch_size_override: Optional[int] = None,
) -> Future[set[UbiUnit]]:
    return _search_units_per_repos(
        or_criteria,
        repos,
        content_type_cls=ModulemdUnit,
        batch_size_override=batch_size_override,
    )


def search_rpms(
    or_criteria: list[Criteria],
    repos: list[YumRepository],
    batch_size_override: Optional[int] = None,
) -> Future[set[UbiUnit]]:
    return _search_units_per_repos(
        or_criteria,
        repos,
        content_type_cls=RpmUnit,
        batch_size_override=batch_size_override,
    )


def search_modulemd_defaults(
    or_criteria: list[Criteria],
    repos: list[YumRepository],
    batch_size_override: Optional[int] = None,
) -> Future[set[UbiUnit]]:
    return _search_units_per_repos(
        or_criteria,
        repos,
        content_type_cls=ModulemdDefaultsUnit,
        batch_size_override=batch_size_override,
    )
