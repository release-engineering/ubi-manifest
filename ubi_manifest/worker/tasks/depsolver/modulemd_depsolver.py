"""
Module for depsolving modulemds in ubi repositories
"""
import logging
import os
from itertools import chain
from typing import Dict, List, Set

from more_executors import Executors
from more_executors.futures import f_proxy
from pubtools.pulplib import YumRepository

from .models import ModularDepsolverItem, UbiUnit
from .pulp_queries import search_modulemd_defaults, search_modulemds
from .utils import get_criteria_for_modules, get_modulemd_output_set, split_filename

_LOG = logging.getLogger(__name__)

MAX_WORKERS = int(os.getenv("UBI_MANIFEST_MODULAR_DEPSOLVER_WORKERS", "8"))


class ModularDepsolver:
    """
    Class for depsolving modulemd units
    """

    def __init__(self, modular_items: List[ModularDepsolverItem]) -> None:
        self._modular_items: List[ModularDepsolverItem] = modular_items
        self._input_repos: List[YumRepository] = list(
            chain.from_iterable(item.in_pulp_repos for item in self._modular_items)
        )
        self._profiles = {}
        for module in chain.from_iterable(
            item.modulelist for item in self._modular_items
        ):
            key = f"{module.name}:{module.stream}"
            self._profiles[key] = module.profiles

        # executor for this class, not adding retries because for pulp
        # we use executor from pulplib
        self._executor = Executors.thread_pool(
            max_workers=MAX_WORKERS, name="modular-depsolver"
        )

        # set of all already searched modules to avoid duplication & cycles
        self._searched_modules: Dict[str, Set[str]] = {
            "without_stream": set(),
            "with_stream": set(),
        }
        # output set of resolved modulemd packages
        self.modules: List[UbiUnit] = []
        # output set of resolved modulemd defaults
        self.default_modulemds: List[UbiUnit] = []
        # set of binary and debuginfo rpm dependencies to be resolved
        self.rpm_dependencies: Set(str) = set()

    def __enter__(self):
        self._executor.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self._executor.__exit__(*args, **kwargs)

    def run(self):
        """
        Run depsolver for each moudular dependency - recursively resolve all of
        its modular dependencies and add binary and debug dependencies to list.
        """
        for item in self._modular_items:
            modulemds_criteria = get_criteria_for_modules(item.modulelist)
            for module in item.modulelist:
                self._update_searched_modules(module)
            modules = f_proxy(
                self._executor.submit(
                    search_modulemds, modulemds_criteria, item.in_pulp_repos
                )
            )
            # recurrently resolve dependencies for found modules
            self._depsolve_modules(modules)

    def _depsolve_modules(self, modules):
        """
        Update modulemd output set with latest versions of modules, then find
        dependencies for the modules and depsolve them.
        """
        filtered_modules = get_modulemd_output_set(modules)
        self.modules.extend(filtered_modules)

        modules_to_search = []
        modulemd_defaults_criteria = get_criteria_for_modules(filtered_modules)
        self.default_modulemds.extend(
            f_proxy(
                self._executor.submit(
                    search_modulemd_defaults,
                    modulemd_defaults_criteria,
                    self._input_repos,
                )
            )
        )

        for module in filtered_modules:
            self._update_rpm_dependencies(module)
            # If dependencies is None, skip it
            if not module.dependencies:
                continue
            # Get all unresolved dependencies
            for dependency in module.dependencies:
                if not self._already_searched(dependency):
                    modules_to_search.append(dependency)
                    self._update_searched_modules(dependency)

        # If there are some unresolved dependencies, get them and depsolve them recursively
        if modules_to_search:
            modulemds_criteria = get_criteria_for_modules(modules_to_search)
            new_modules = f_proxy(
                self._executor.submit(
                    search_modulemds, modulemds_criteria, self._input_repos
                )
            )
            self._depsolve_modules(new_modules)

    def _update_searched_modules(self, module):
        if module.stream is None:
            self._searched_modules["without_stream"].add(module.name)
        else:
            self._searched_modules["with_stream"].add(f"{module.name}:{module.stream}")

    def _already_searched(self, module):
        """Returns True if the module has not yet been searched for"""
        return (
            module.name in self._searched_modules["without_stream"]
            or f"{module.name}:{module.stream}" in self._searched_modules["with_stream"]
        )

    def _update_rpm_dependencies(self, module):
        """
        Adds all pkgs from artifacts to rpm dependencies. Omits source pkgs and
        filters by profiles if available.
        """
        if module.artifacts:
            pkg_names = []
            key = f"{module.name}:{module.stream}"
            if module.profiles:
                for profile in self._profiles.get(key) or []:
                    pkg_names.extend(module.profiles.get(profile) or [])

            for pkg in module.artifacts_filenames:
                # filter by profile if available
                if pkg_names:
                    name, _, _, _, _ = split_filename(pkg)
                    if name not in pkg_names:
                        continue

                self.rpm_dependencies.add(pkg)

    def export(self) -> Dict[str, Dict[str, List[UbiUnit]]]:
        """Returns a dictionary of depsolved modules and their rpm dependencies."""
        out = {}
        modules_out = {}
        nsvca_set = set()
        for module in self.modules:
            # filter duplicates
            if not module.nsvca in nsvca_set:
                nsvca_set.add(module.nsvca)
                modules_out.setdefault(module.associate_source_repo_id, []).append(
                    module
                )

        # append ModulemdDefaultsUnits to output set
        def_mod_ids = set()
        for def_mod in self.default_modulemds:
            # filter duplicates
            if def_mod.unit_id not in def_mod_ids:
                def_mod_ids.add(f"{def_mod.name}:{def_mod.stream}")
                modules_out.setdefault(def_mod.associate_source_repo_id, []).append(
                    def_mod
                )

        out["rpm_dependencies"] = self.rpm_dependencies
        out["modules_out"] = modules_out
        return out
