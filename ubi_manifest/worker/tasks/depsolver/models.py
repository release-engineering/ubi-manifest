from typing import List, Set

from attrs import define
from pubtools.pulplib import YumRepository
from ubiconfig.config_types.modules import Module


class UbiUnit:
    """
    Wrapping class of model classes (*Unit) of pubtools.pulplib.
    """

    def __init__(self, unit, src_repo_id):
        self._unit = unit
        self.associate_source_repo_id = src_repo_id

    def __getattr__(self, name):
        return getattr(self._unit, name)

    def __str__(self):
        return str(self._unit)

    def isinstance_inner_unit(self, klass):
        return isinstance(self._unit, klass)

    # TODO make this return hash of self._unit if possible in future
    # it should help us with not adding the same units into sets
    # that differ with associate_source_repo_id attr only
    # currently some *Unit classes from pulplib are not hashable
    # def __hash__(self):
    #    return hash(self._unit)


@define
class PackageToExclude:
    name: str
    globbing: bool = False
    arch: str = None


@define
class DepsolverItem:
    whitelist: Set[str]
    blacklist: List[PackageToExclude]
    in_pulp_repos: List[YumRepository]


@define
class ModularDepsolverItem:
    modulelist: List[Module]
    repo: YumRepository
    in_pulp_repos: List[YumRepository]
