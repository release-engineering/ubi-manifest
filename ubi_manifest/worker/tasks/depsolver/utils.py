import re
from collections import defaultdict, deque
from itertools import chain
from logging import getLogger
from typing import Dict, List, Tuple

from pubtools.pulplib import Client, Criteria, Matcher
from rpm import labelCompare as label_compare  # pylint: disable=no-name-in-module
from ubiconfig import UbiConfig

from ubi_manifest.worker.tasks.depsolver.models import PackageToExclude

_LOG = getLogger(__name__)

OPEN_END_ONE_OR_MORE_PAR_REGEX = re.compile(r"^\(+|\)+$")
OPERATOR_BOOL_REGEX = re.compile(r"if|else|and|or|unless|with|without")
OPERATOR_NUM_REGEX = re.compile(r"<|<=|=|>|>=")


def make_pulp_client(url, username, password, insecure):
    auth = None

    if username:
        auth = (username, password)

    return Client(url, auth=auth, verify=not insecure)


def create_or_criteria(fields, values):
    # fields - list/tuple of fields [field1, field2]
    # values - list of tuples [(field1 value, field2 value), ...]
    # creates criteria for pulp query in a following way
    # one tuple in values uses AND logic
    # each criteria for one tuple are agregated by to or_criteria list
    or_criteria = []

    for val_tuple in values:
        inner_and_criteria = []
        if len(val_tuple) != len(fields):
            raise ValueError
        for index, field in enumerate(fields):
            inner_and_criteria.append(Criteria.with_field(field, val_tuple[index]))

        or_criteria.append(Criteria.and_(*inner_and_criteria))

    return or_criteria


def flatten_list_of_sets(list_of_sets):
    out = set()
    for one_set in list_of_sets:
        out |= one_set

    return out


def _is_blacklisted(package, blacklist):
    for item in blacklist:
        blacklisted = False
        if item.globbing:
            if package.name.startswith(item.name):
                blacklisted = True
        else:
            if package.name == item.name:
                blacklisted = True
        if item.arch:
            if package.arch != item.arch:
                blacklisted = False

        if blacklisted:
            return blacklisted


def get_n_latest_from_content(content, blacklist, modular_rpms=None):
    name_rpms_maps = {}
    for item in content:
        if modular_rpms:
            if item.filename in modular_rpms:
                _LOG.debug("Skipping modular RPM %s", item.filename)
                continue

        if _is_blacklisted(item, blacklist):
            continue

        name_rpms_maps.setdefault(item.name, []).append(item)

    out = []
    for rpm_list in name_rpms_maps.values():
        rpm_list.sort(key=vercmp_sort())
        _keep_n_latest_rpms(rpm_list)
        out.extend(rpm_list)

    return out


def parse_bool_deps(bool_dependency):
    """Parses boolean/rich dependency clause and returns set of names of packages"""
    to_parse = bool_dependency.split()

    skip_next = False
    pkg_names = set()

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

        pkg_names.add(item)
    return pkg_names


def vercmp_sort():
    class Klass:
        def __init__(self, package):
            self.evr_tuple = (package.epoch, package.version, package.release)

        def __lt__(self, other):
            return label_compare(self.evr_tuple, other.evr_tuple) < 0

        def __gt__(self, other):
            return label_compare(self.evr_tuple, other.evr_tuple) > 0

        def __eq__(self, other):
            return label_compare(self.evr_tuple, other.evr_tuple) == 0

        def __le__(self, other):
            return label_compare(self.evr_tuple, other.evr_tuple) <= 0

        def __ge__(self, other):
            return label_compare(self.evr_tuple, other.evr_tuple) >= 0

        def __ne__(self, other):
            return label_compare(self.evr_tuple, other.evr_tuple) != 0

    return Klass


def _keep_n_latest_rpms(rpms, n=1):
    """
    Keep n latest non-modular rpms.

    Arguments:
        rpms (List[Rpm]): Sorted, oldest goes first

    Keyword arguments:
        n (int): Number of non-modular package versions to keep

    Returns:
        None. The packages list is changed in-place
    """
    # Use a queue of n elements per arch
    pkgs_per_arch = defaultdict(lambda: deque(maxlen=n))

    for rpm in rpms:
        pkgs_per_arch[rpm.arch].append(rpm)

    latest_pkgs_per_arch = list(chain.from_iterable(pkgs_per_arch.values()))

    rpms[:] = latest_pkgs_per_arch


# borrowed from https://github.com/rpm-software-management/yum
def split_filename(filename: str) -> Tuple[str]:
    """
    Pass in a standard style rpm fullname

    Return a name, version, release, epoch, arch, e.g.::
        foo-1.0-1.i386.rpm returns foo, 1.0, 1, i386
        1:bar-9-123a.ia64.rpm returns bar, 9, 123a, 1, ia64
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


def remap_keys(mapping: Dict, dict_to_remap: Dict) -> Dict:
    out = {}
    for k, v in dict_to_remap.items():
        new_key = mapping[k]
        out.setdefault(new_key, []).extend(v)

    return out


def parse_blacklist_config(ubi_config: UbiConfig) -> List[PackageToExclude]:
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


def keep_n_latest_modules(modules, n=1):
    """
    Keeps n latest modules in modules sorted list
    """
    modules_to_keep = []
    versions_to_keep = sorted(set(m.version for m in modules))[-n:]

    for module in modules:
        if module.version in versions_to_keep:
            modules_to_keep.append(module)

    modules[:] = modules_to_keep


def get_modulemd_output_set(modules):
    """
    Take all modular packages and for each package and stream return only the
    latest version of it.
    """
    name_stream_modules_map = {}
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


def get_criteria_for_modules(modules):
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

    fields = ("name", "stream")
    or_criteria = create_or_criteria(fields, criteria_values)
    return or_criteria
