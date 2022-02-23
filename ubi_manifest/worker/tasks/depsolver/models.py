from typing import List

from attrs import define
from pubtools.pulplib import YumRepository


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

    # TODO make this return hash of self._unit if possible in future
    # it should help us with not adding the same units into sets
    # that differ with associate_source_repo_id attr only
    # currently some *Unit classes from pulplib are not hashable
    # def __hash__(self):
    #    return hash(self._unit)


@define
class UbiRepository:
    whitelist: List[str]
    resolved: List[UbiUnit]
    in_pulp_repo: YumRepository
    out_pulp_repo: YumRepository
