from typing import Optional, Any

from attrs import define
from pubtools.pulplib import YumRepository, Unit
from ubiconfig.config_types.modules import Module


class UbiUnit:
    """
    Wrapping class of model classes (*Unit) of pubtools.pulplib.
    """

    def __init__(self, unit: Unit, src_repo_id: str):
        self._unit = unit
        self.associate_source_repo_id = src_repo_id

    def __getattr__(self, name: str) -> Any:
        return getattr(self._unit, name)

    def __str__(self) -> str:
        return str(self._unit)

    def isinstance_inner_unit(self, klass: Unit) -> bool:
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
    arch: Optional[str] = None


@define
class DepsolverItem:
    whitelist: set[str]
    blacklist: list[PackageToExclude]
    in_pulp_repos: list[YumRepository]


@define
class ModularDepsolverItem:
    modulelist: list[Module]
    repo: YumRepository
    in_pulp_repos: list[YumRepository]
