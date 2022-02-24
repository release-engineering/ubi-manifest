import re
from collections import defaultdict, deque
from itertools import chain
from logging import getLogger

from pubtools.pulplib import Criteria
from rpm import labelCompare as label_compare  # pylint: disable=no-name-in-module

_LOG = getLogger(__name__)


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


def get_n_latest_from_content(content, modular_rpms=None):
    name_rpms_maps = {}
    for item in content:
        if modular_rpms:
            if item.filename in modular_rpms:
                _LOG.debug("Skipping modular RPM %s", item.filename)
                continue

        name_rpms_maps.setdefault(item.name, []).append(item)

    out = []
    for rpm_list in name_rpms_maps.values():
        rpm_list.sort(key=vercmp_sort())
        _keep_n_latest_rpms(rpm_list)
        out.extend(rpm_list)

    return out


def parse_bool_deps(bool_dependency):
    """Parses bool/rich dependency clause and returns set of names of packages"""
    # remove all paranthesis from clause
    _dep = re.sub(r"\(|\)", "", bool_dependency)
    to_parse = _dep.split()

    operators = set(
        [
            "if",
            "else",
            "and",
            "or",
            "unless",
            "with",
            "without",
        ]
    )

    operator_num = set(["<", "<=", "=", ">", ">="])
    skip_next = False
    pkg_names = set()
    # nested = 0
    for item in to_parse:
        # skip item imediately apearing after num operator
        if skip_next:
            skip_next = False
            continue
        # skip operator
        if item in operators:
            continue

        # after num operator there is usually evr, we want to skip that as well
        if item in operator_num:
            skip_next = True
            continue

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
