from __future__ import annotations

import os
import re
from collections import defaultdict, deque
from itertools import chain
from logging import getLogger
from typing import Any, Optional

from pubtools.pulplib import Client, Criteria, Matcher, RpmDependency
from ubiconfig import UbiConfig

from ubi_manifest.worker.models import PackageToExclude, UbiUnit

_LOG = getLogger(__name__)

try:
    from rpm import labelCompare as label_compare  # pylint: disable=no-name-in-module
except ImportError as ex:  # pragma: no cover
    _LOG.error("Cannot import rpm module, please install rpm python bindings")


OPEN_END_ONE_OR_MORE_PAR_REGEX = re.compile(r"^\(+|\)+$")
OPERATOR_BOOL_REGEX = re.compile(r"if|else|and|or|unless|with|without")
OPERATOR_NUM_REGEX = re.compile(r"<|<=|=|>|>=")

RELATION_CMP_MAP = {
    "GT": lambda x, y: label_compare(x, y) > 0,
    "GE": lambda x, y: label_compare(x, y) >= 0,
    "EQ": lambda x, y: label_compare(x, y) == 0,
    "LE": lambda x, y: label_compare(x, y) <= 0,
    "LT": lambda x, y: label_compare(x, y) < 0,
}


def make_pulp_client(config: dict[str, Any]) -> Client:
    """
    Create and return a Pulp client from given configuration.
    """
    kwargs = {"verify": config.get("pulp_verify")}
    cert, key = config.get("pulp_cert"), config.get("pulp_key")
    # check cert/key for presence, if present assume cert/key for pulp auth
    if os.path.isfile(cert) and os.path.isfile(key):  # type: ignore [arg-type]
        kwargs["cert"] = (cert, key)

    # if cert/key not present, use user/pass auth to pulp
    else:
        kwargs["auth"] = (config.get("pulp_username"), config.get("pulp_password"))

    return Client(config.get("pulp_url"), **kwargs)


def create_or_criteria(
    fields: list[str], values: list[tuple[Any, ...]]
) -> list[Criteria]:
    """
    Creates a list of Pulp 'AND' criteria, joining inner criteria for given
    fields and corresponding values.

    fields - list of fields [field1, field2]
    values - list of tuples [(field1 value, field2 value), ...]
    """
    or_criteria: list[Criteria] = []

    for val_tuple in values:
        inner_and_criteria = []
        if len(val_tuple) != len(fields):
            raise ValueError
        for index, field in enumerate(fields):
            inner_and_criteria.append(Criteria.with_field(field, val_tuple[index]))

        or_criteria.append(Criteria.and_(*inner_and_criteria))

    return or_criteria


def flatten_list_of_sets(list_of_sets: list[set[Any]]) -> set[Any]:
    """
    Converts a list of sets into a single set.
    """
    out = set()
    for one_set in list_of_sets:
        out |= one_set

    return out


def is_blacklisted(package: UbiUnit, blacklist: list[PackageToExclude]) -> bool:
    """
    Determines whether or not given package is blacklisted.
    """
    for item in blacklist:
        if item.arch:
            if package.arch != item.arch:
                continue

        if item.globbing:
            if package.name.startswith(item.name):
                return True
        else:
            if package.name == item.name:
                return True
    return False


def get_n_latest_from_content(
    content: set[UbiUnit],
    blacklist: list[PackageToExclude],
    modular_rpms: Optional[set[str]] = None,
) -> list[UbiUnit]:
    """
    Filters modular, blacklisted, and outdated RPMs from given content.
    """
    name_rpms_maps: dict[str, list[UbiUnit]] = {}
    for item in content:
        if modular_rpms:
            if item.filename in modular_rpms:
                _LOG.debug("Skipping modular RPM %s", item.filename)
                continue

        if is_blacklisted(item, blacklist):
            continue

        name_rpms_maps.setdefault(item.name, []).append(item)

    out = []
    for rpm_list in name_rpms_maps.values():
        keep_n_latest_rpms(rpm_list)
        out.extend(rpm_list)

    return out


def parse_bool_deps(bool_dependency: str) -> set[RpmDependency]:
    """
    Parses boolean/rich dependency clause and returns set of names of packages.
    """
    to_parse = bool_dependency.split()

    skip_next = False
    out = set()

    for item in to_parse:
        # skip item immediately apearing after num operator
        if skip_next:
            skip_next = False
            continue
        # skip operator
        if re.match(OPERATOR_BOOL_REGEX, item):
            continue

        # after num operator there is usually evr, we want to skip that as well
        if re.match(OPERATOR_NUM_REGEX, item):
            skip_next = True
            continue
        # remove all starting and ending paranthesis there can some left when using nesting
        item = re.sub(OPEN_END_ONE_OR_MORE_PAR_REGEX, "", item)
        # after all substitutions we ended with empty string, continue to the next item
        if not item:
            continue
        # if there is wanted opening parenthesis in item, let's add ending parenthesis
        # which we removed in the previous step
        # in order not to brake the item name
        if "(" in item:
            item += ")"

        out.add(RpmDependency(name=item))
    return out


def vercmp_sort() -> Any:
    """
    Creates and returns a wrapper class enabling sorting/comparing UbiUnits
    by epoc, version, release tuple (evr).
    """

    class Klass:
        """
        Wrapper class for UbiUnits that enables sorting/comparing.
        """

        def __init__(self, package: UbiUnit):
            self.evr_tuple = (package.epoch, package.version, package.release)

        def __lt__(self, other: Klass) -> Any:
            return label_compare(self.evr_tuple, other.evr_tuple) < 0

        def __gt__(self, other: Klass) -> Any:
            return label_compare(self.evr_tuple, other.evr_tuple) > 0

        def __eq__(self, other: Klass) -> Any:  # type: ignore[override]
            return label_compare(self.evr_tuple, other.evr_tuple) == 0

        def __le__(self, other: Klass) -> Any:
            return label_compare(self.evr_tuple, other.evr_tuple) <= 0

        def __ge__(self, other: Klass) -> Any:
            return label_compare(self.evr_tuple, other.evr_tuple) >= 0

        def __ne__(self, other: Klass) -> Any:  # type: ignore[override]
            return label_compare(self.evr_tuple, other.evr_tuple) != 0

    return Klass


def is_requirement_resolved(req: RpmDependency, provider: RpmDependency) -> Any:
    """
    Determines whether or not a given requirement has been resolved.
    """
    if req.flags:
        req_evr = (req.epoch, req.version, req.release)
        prov_evr = (provider.epoch, provider.version, provider.release)
        # compare provider with requirement
        out = RELATION_CMP_MAP[req.flags](prov_evr, req_evr)  # type: ignore [no-untyped-call]

    else:
        # without flags we just compare names
        out = req.name == provider.name

    return out


def keep_n_latest_rpms(rpms: list[UbiUnit], n: int = 1) -> None:
    """
    Keep n latest non-modular rpms. If there are rpms with different arches
    only pkgs with `n` highest versions are kept.

    Arguments:
        rpms (list[Rpm]): List of rpms

    Keyword arguments:
        n (int): Number of non-modular package versions to keep

    Returns:
        None. The packages list is changed in-place
    """
    # Use a queue of n elements per arch
    pkgs_per_arch: dict[str, Any] = defaultdict(lambda: deque(maxlen=n))

    # set of allowed (version, release) tuples
    allowed_ver_rel = set()
    for rpm in sorted(rpms, key=vercmp_sort(), reverse=True):
        allowed_ver_rel.add(
            (
                rpm.version,
                rpm.release,
            )
        )
        if len(allowed_ver_rel) > n:
            break

        if (
            rpm.version,
            rpm.release,
        ) in allowed_ver_rel:
            pkgs_per_arch[rpm.arch].append(rpm)

    latest_pkgs_per_arch = list(chain.from_iterable(pkgs_per_arch.values()))

    rpms[:] = latest_pkgs_per_arch


# borrowed from https://github.com/rpm-software-management/yum
def split_filename(filename: str) -> tuple[str, ...]:
    """
    Returns a name, version, release, epoch, arch tuple for a standard RPM fullname.

    E.g.;
        foo-1.0-1.i386.rpm -> foo, 1.0, 1, i386
        1:bar-9-123a.ia64.rpm -> bar, 9, 123a, 1, ia64
    """
    if filename[-4:] == ".rpm":
        filename = filename[:-4]

    arch_index = filename.rfind(".")
    arch = filename[arch_index + 1 :]

    rel_index = filename[:arch_index].rfind("-")
    rel = filename[rel_index + 1 : arch_index]

    ver_index = filename[:rel_index].rfind("-")
    ver = filename[ver_index + 1 : rel_index]

    epoch_index = filename.find(":")

    if epoch_index == -1:
        epoch = ""
    else:
        epoch = filename[:epoch_index]

    name = filename[epoch_index + 1 : ver_index]

    return name, ver, rel, epoch, arch


def remap_keys(
    mapping: dict[str, str], dict_to_remap: dict[str, list[UbiUnit]]
) -> dict[str, list[UbiUnit]]:
    """
    Remaps given `dict_to_remap` according to `mapping` values.

    E.g., mapping["A", "1"], dict_to_remap["A", list[...]] == output["1", list[...]]
    """
    out: dict[str, list[UbiUnit]] = {}
    for k, v in dict_to_remap.items():
        new_key = mapping[k]
        out.setdefault(new_key, []).extend(v)

    return out


def parse_blacklist_config(ubi_config: UbiConfig) -> list[PackageToExclude]:
    """
    Produces a list of `PackagesToExclude` based on given `UbiConfig`.
    """
    packages_to_exclude = []
    for package_pattern in ubi_config.packages.blacklist:
        globbing = package_pattern.name.endswith("*")
        if globbing:
            name = package_pattern.name[:-1]
        else:
            name = package_pattern.name
        arch = None if package_pattern.arch in ("*", None) else package_pattern.arch

        packages_to_exclude.append(PackageToExclude(name, globbing, arch))

    return packages_to_exclude


def keep_n_latest_modules(modules: list[UbiUnit], n: int = 1) -> None:
    """
    Keeps n latest modules in modules sorted list.
    """
    modules_to_keep = []
    versions_to_keep = sorted(set(m.version for m in modules))[-n:]

    for module in modules:
        if module.version in versions_to_keep:
            modules_to_keep.append(module)

    modules[:] = modules_to_keep


def get_modulemd_output_set(modules: set[UbiUnit]) -> list[UbiUnit]:
    """
    Take all modular packages and for each package and stream return only the
    latest version of it.
    """
    name_stream_modules_map: dict[str, list[UbiUnit]] = {}
    # create internal dict structure for easier sorting
    # mapping "name + stream": list of modules
    for modulemd in modules:
        key = modulemd.name + modulemd.stream
        name_stream_modules_map.setdefault(key, []).append(modulemd)

    out = []
    # sort rpms and keep N latest versions of them
    for module_list in name_stream_modules_map.values():
        module_list.sort(key=lambda module: module.version)
        keep_n_latest_modules(module_list)
        out.extend(module_list)

    return out


def get_criteria_for_modules(modules: list[UbiUnit]) -> list[Criteria]:
    """
    Creates OR criteria that search for all modules by name and stream. If the
    module has empty stream field, all modules with the corresponding name will be matched.
    """
    criteria_values = []
    for module in modules:
        if module.stream:
            criteria_values.append(
                (
                    module.name,
                    module.stream,
                )
            )
        else:
            criteria_values.append(
                (
                    module.name,
                    Matcher.exists(),
                )
            )

    fields = ["name", "stream"]
    or_criteria = create_or_criteria(fields, criteria_values)
    return or_criteria


def keep_n_latest_modulemd_defaults(
    modulemd_defaults: list[UbiUnit], n: int = 1
) -> None:
    """
    Keeps n latest modulemd_defaults units, determined by greatest key in profiles.
    """

    # group defaults units in lists mapped to name+stream
    mdd_map = defaultdict(list)
    for mdd in modulemd_defaults:
        mdd_map[mdd.name + mdd.stream].append(mdd)

    # get the 'latest' unit belonging to each name+stream
    module_defaults_to_keep = []
    for mdd_list in mdd_map.values():
        module_defaults_to_keep.extend(
            sorted(mdd_list, key=lambda x: x.profiles.keys())[-n:]
        )

    modulemd_defaults[:] = module_defaults_to_keep
