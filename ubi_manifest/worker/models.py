from typing import Any, Optional

from attrs import define
from pubtools.pulplib import Unit, YumRepository
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
        """
        Compares type of unit given to type of unit this UbiUnit was derived from.
        """
        return isinstance(self._unit, klass)

    def __hash__(self) -> int:
        return hash(self._unit)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, UbiUnit):
            return (self.__hash__() == other.__hash__()) and (
                self.associate_source_repo_id == other.associate_source_repo_id
            )
        return NotImplemented


@define
class PackageToExclude:
    """
    Representation of a excluded/blacklisted package.
    """

    name: str
    globbing: bool = False
    arch: Optional[str] = None


@define
class DepsolverItem:
    """
    Item for resolution by RPM depsolver.
    """

    whitelist: set[str]
    blacklist: list[PackageToExclude]
    in_pulp_repos: list[YumRepository]


@define
class ModularDepsolverItem:
    """
    Item for resolution by modulemd depsolver.
    """

    modulelist: list[Module]
    repo: YumRepository
    in_pulp_repos: list[YumRepository]
