from itertools import chain

import pytest
from more_executors.futures import f_proxy
from pubtools.pulplib import (
    ModulemdDefaultsUnit,
    ModulemdDependency,
    ModulemdUnit,
    RpmUnit,
)
from ubiconfig.config_types.modules import Module

from ubi_manifest.worker.tasks.depsolver.models import (
    DepsolverItem,
    ModularDepsolverItem,
    PackageToExclude,
    UbiUnit,
)
from ubi_manifest.worker.tasks.depsolver.modulemd_depsolver import ModularDepsolver

from .utils import create_and_insert_repo


def test_export(modular_depsolver):
    """Test exporting from ModularDepsolver"""

    # Define mock UbiUnits
    module1 = ModulemdUnit(
        name="perl-YAML",
        stream="1.24",
        version=8030020200313080146,
        context="b7fad3bf",
        arch="x86_64",
    )
    unit1 = UbiUnit(module1, "test_repo1")

    module2 = ModulemdUnit(
        name="perl",
        stream="5.30",
        version=8040020200923213406,
        context="466ea64f",
        arch="x86_64",
    )
    unit2 = UbiUnit(module2, "test_repo1")

    module_def1 = ModulemdDefaultsUnit(
        name="perl",
        stream="6.30",
        repo_id="test_repo1",
        repository_memberships=["test_repo1"],
    )
    unit_def1 = UbiUnit(module_def1, "test_repo1")

    module_def2 = ModulemdDefaultsUnit(
        name="perl-YAML",
        stream="1.24",
        repo_id="test_repo1",
        repository_memberships=["test_repo1"],
    )
    unit_def2 = UbiUnit(module_def2, "test_repo1")

    module3 = ModulemdUnit(
        name="perl",
        stream="6.30",
        version=3,
        context="ABC",
        arch="x86_64",
    )
    unit3 = UbiUnit(module3, "test_repo2")

    # this is copy of module3 to test that duplicates are skipped
    module4 = ModulemdUnit(
        name="perl",
        stream="6.30",
        version=3,
        context="ABC",
        arch="x86_64",
    )
    unit4 = UbiUnit(module4, "test_repo2")

    rpm_units = {
        "perl-4:5.30.1-452.module+el8.4.0+8990+01326e37.src",
        "perl-4:5.30.1-452.module+el8.4.0+8990+01326e37.x86_64",
        "perl-archive-tar-0:2.32-440.module+el8.3.0+6718+7f269185.src",
        "perl-archive-zip-0:1.67-1.module+el8.3.0+6718+7f269185.noarch",
        "nodejs-debuginfo-1:10.24.0-1.module+el8.3.0+10166+b07ac28e.x86_64",
        "nodejs-debugsource-1:10.24.0-1.module+el8.3.0+10166+b07ac28e.x86_64",
    }

    expected_out = {}
    expected_out["modules_out"] = {
        "test_repo1": [unit1, unit2, unit_def1, unit_def2],
        "test_repo2": [unit3],
    }
    expected_out["rpm_dependencies"] = rpm_units

    modular_depsolver.modules = [unit1, unit2, unit3, unit4]
    modular_depsolver.default_modulemds = [unit_def1, unit_def2]
    modular_depsolver.rpm_dependencies = rpm_units
    dep_out = modular_depsolver.export()

    assert expected_out == dep_out


def test_run(pulp):
    """Test the main method of ModularDepsolver."""

    expected_out = _prepare_pulp(pulp)
    repo1 = expected_out["repos"][0]
    repo2 = expected_out["repos"][1]

    modulelist1 = [Module("perl-YAML", "1.24")]
    in_pulp_repos1 = [repo1]
    mod_dep_item1 = ModularDepsolverItem(modulelist1, repo1, in_pulp_repos1)

    modulelist2 = [Module("module_in_second_repo", "8.30", ["test"])]
    in_pulp_repos2 = [repo2]
    mod_dep_item2 = ModularDepsolverItem(modulelist2, repo2, in_pulp_repos2)

    with ModularDepsolver([mod_dep_item1, mod_dep_item2]) as depsolver:
        depsolver.run()

        # all the modules should be searched
        assert depsolver._searched_modules["with_stream"] == set(
            f"{x[0]}:{x[1]}"
            for x in expected_out["modulemds"]
            if not x[0] == "test_none_in_stream"
        )
        assert depsolver._searched_modules["without_stream"] == set(
            f"{x[0]}"
            for x in expected_out["modulemds"]
            if x[0] == "test_none_in_stream"
        )

        assert len(depsolver.modules) == len(expected_out["modulemds"])

        output_modules = list(
            (x.name, x.stream, x.associate_source_repo_id) for x in depsolver.modules
        )
        output_modules.sort(key=lambda x: (x[0], x[1]))
        assert output_modules == expected_out["modulemds"]

        # Check that the modulemdDefaults have been resolved
        assert len(depsolver.default_modulemds) == 4

        output_def_modules = list(
            (x.name, x.stream, x.repo_id) for x in depsolver.default_modulemds
        )
        output_def_modules.sort(key=lambda x: (x[0], x[1]))
        assert output_def_modules == expected_out["modulemd_defaults"]

        # check the modular rpms:
        assert depsolver.rpm_dependencies == expected_out["modular_rpms"]


def _prepare_pulp(pulp):

    # Define mock repos
    repo_1 = create_and_insert_repo(id="test_repo_1", pulp=pulp)

    repo_2 = create_and_insert_repo(id="test_repo_2", pulp=pulp)

    # Define mock module units for repo_1
    module1 = ModulemdUnit(
        name="perl-YAML",
        stream="1.24",
        version=8030020200313080146,
        context="b7fad3bf",
        arch="x86_64",
        artifacts=[
            "perl-YAML-0:1.24-3.module+el8.1.0+2934+dec45db7.noarch",
            "perl-YAML-0:1.24-3.module+el8.1.0+2934+dec45db7.src",
        ],
        dependencies=[
            ModulemdDependency(name="perl", stream="5.30"),
            ModulemdDependency(name="dependency_1", stream="11.1"),
        ],
    )

    module2 = ModulemdUnit(
        name="perl",
        stream="5.30",
        version=8040020200923213406,
        context="466ea64f",
        arch="x86_64",
        artifacts=[
            "perl-4:5.30.1-452.module+el8.4.0+8990+01326e37.src",
            "perl-4:5.30.1-452.module+el8.4.0+8990+01326e37.x86_64",
            "perl-archive-tar-0:2.32-440.module+el8.3.0+6718+7f269185.src",
            "perl-archive-zip-0:1.67-1.module+el8.3.0+6718+7f269185.noarch",
            "nodejs-debuginfo-1:10.24.0-1.module+el8.3.0+10166+b07ac28e.x86_64",
            "nodejs-debugsource-1:10.24.0-1.module+el8.3.0+10166+b07ac28e.x86_64",
        ],
        dependencies=[
            ModulemdDependency(name="dependency_2", stream="2.22"),
            ModulemdDependency(name="dependency_1", stream="11.1"),
        ],
    )

    ignored_module = ModulemdUnit(
        name="ignored",
        stream="6.30",
        version=3,
        context="ABC",
        arch="x86_64",
        artifacts=[
            "ignored-archive-tar-0:2.32-440.module+el8.3.0+6718+7f269185.src",
            "ignored-archive-zip-0:1.67-1.module+el8.3.0+6718+7f269185.noarch",
        ],
    )

    module_dep_1 = ModulemdUnit(
        name="dependency_1",
        stream="11.1",
        version=1,
        context="def",
        arch="x86_64",
        artifacts=[
            "dep1-4:5.30.1-452.module+el8.4.0+8990+01326e37.src",
            "dep1-4:5.30.1-452.module+el8.4.0+8990+01326e37.x86_64",
            "dep1-archive-tar-0:2.32-440.module+el8.3.0+6718+7f269185.src",
            "dep1-archive-zip-0:1.67-1.module+el8.3.0+6718+7f269185.noarch",
        ],
    )

    module_dep_2 = ModulemdUnit(
        name="dependency_2",
        stream="2.22",
        version=2,
        context="efg",
        arch="x86_64",
        artifacts=[
            "dep1-4:5.30.1-452.module+el8.4.0+8990+01326e37.src",
            "dep1-4:5.30.1-452.module+el8.4.0+8990+01326e37.x86_64",
            "dep1-archive-tar-0:2.32-440.module+el8.3.0+6718+7f269185.src",
            "dep1-archive-zip-0:1.67-1.module+el8.3.0+6718+7f269185.noarch",
        ],
    )

    module_def1 = ModulemdDefaultsUnit(
        name="perl-YAML",
        stream="1.24",
        repo_id="test_repo_1",
        repository_memberships=["test_repo_1"],
    )

    module_def2 = ModulemdDefaultsUnit(
        name="perl",
        stream="5.30",
        repo_id="test_repo_1",
        repository_memberships=["test_repo_1"],
    )

    ignored_module_def = ModulemdDefaultsUnit(
        name="ignored",
        stream="6.30",
        repo_id="test_repo_1",
        repository_memberships=["test_repo_1"],
    )

    module_dep_def1 = ModulemdDefaultsUnit(
        name="dependency_1",
        stream="11.1",
        repo_id="test_repo_1",
        repository_memberships=["test_repo_1"],
    )

    module_dep_def2 = ModulemdDefaultsUnit(
        name="dependency_2",
        stream="2.22",
        repo_id="test_repo_1",
        repository_memberships=["test_repo_1"],
    )

    repo_1_units = [
        module1,
        module2,
        module_dep_1,
        module_dep_2,
        ignored_module,
        module_def1,
        module_def2,
        module_dep_def1,
        module_dep_def2,
        ignored_module_def,
    ]
    pulp.insert_units(repo_1, repo_1_units)

    # Define mock module units for repo_2
    module4 = ModulemdUnit(
        name="module_in_second_repo",
        stream="8.30",
        version=6,
        context="fgh",
        arch="x86_64",
        artifacts=[
            "secondRepoRPM-4:5.30.1-452.module+el8.4.0+8990+01326e37.src",
            "secondRepoRPM-4:5.30.1-452.module+el8.4.0+8990+01326e37.x86_64",
            "this-is-not-in-profile-4:5.30.1-452.module+el8.4.0+8990+01326e37.x86_64",
        ],
        dependencies=[
            ModulemdDependency(
                name="dependency_2", stream="something completely different"
            ),
            ModulemdDependency(name="test_none_in_stream", stream=None),
            ModulemdDependency(name="dependency_1", stream="11.1"),
        ],
        profiles={"test": ["secondRepoRPM"]},
    )

    module_dep_3 = ModulemdUnit(
        name="dependency_2",
        stream="something completely different",
        version=3,
        context="efg",
        arch="x86_64",
        artifacts=[
            "dep1-4:5.30.1-452.module+el8.4.0+8990+01326e37.src",
            "dep1-4:5.30.1-452.module+el8.4.0+8990+01326e37.x86_64",
            "dep1-archive-tar-0:2.32-440.module+el8.3.0+6718+7f269185.src",
            "dep1-archive-zip-0:1.67-1.module+el8.3.0+6718+7f269185.noarch",
        ],
    )

    module_dep_4 = ModulemdUnit(
        name="test_none_in_stream",
        stream="v1",
        version=3,
        context="efg",
        arch="x86_64",
        artifacts=[
            "dep3-4:5.30.1-452.module+el8.4.0+8990+01326e37.src",
            "dep3-4:5.30.1-452.module+el8.4.0+8990+01326e37.x86_64",
            "dep3-archive-tar-0:2.32-440.module+el8.3.0+6718+7f269185.src",
            "dep3-archive-zip-0:1.67-1.module+el8.3.0+6718+7f269185.noarch",
        ],
    )

    module_dep_5 = ModulemdUnit(
        name="test_none_in_stream",
        stream="v2",
        version=3,
        context="efg",
        arch="x86_64",
        artifacts=[
            "dep3-4:5.30.1-452.module+el8.4.0+8990+01326e37.src",
            "dep3-4:5.30.1-452.module+el8.4.0+8990+01326e37.x86_64",
            "dep3-archive-tar-0:2.32-440.module+el8.3.0+6718+7f269185.src",
            "dep3-archive-zip-0:1.67-1.module+el8.3.0+6718+7f269185.noarch",
        ],
    )
    repo_2_units = [module4, module_dep_3, module_dep_4, module_dep_5]
    pulp.insert_units(repo_2, repo_2_units)

    expected_modulemds = list(
        [
            (module.name, module.stream, repo_1.id)
            for module in repo_1_units
            if isinstance(module, ModulemdUnit) and not module.name == "ignored"
        ]
        + [(module.name, module.stream, repo_2.id) for module in repo_2_units]
    )

    expected_modulemd_defaults = list(
        [
            (module.name, module.stream, module.repo_id)
            for module in repo_1_units
            if isinstance(module, ModulemdDefaultsUnit) and not module.name == "ignored"
        ]
    )
    expected_modulemd_defaults.sort(key=lambda x: (x[0], x[1]))
    expected_modulemds.sort(key=lambda x: (x[0], x[1]))

    modular_rpms = set(
        chain.from_iterable(
            [
                module.artifacts_filenames
                for module in repo_1_units + repo_2_units
                if isinstance(module, ModulemdUnit) and not module.name == "ignored"
            ]
        )
    )
    # Filter out .src modular rpms
    expected_modular_rpms = set(
        filter(
            lambda x: not x.endswith(".src.rpm") and not "not-in-profile" in x,
            modular_rpms,
        )
    )

    return {
        "repos": [repo_1, repo_2],
        "modulemds": expected_modulemds,
        "modulemd_defaults": expected_modulemd_defaults,
        "modular_rpms": expected_modular_rpms,
    }


@pytest.fixture
def modular_depsolver(pulp):
    """Returns simple ModularDepsolver instance"""
    # Prepare modular depsolver item
    repo1 = create_and_insert_repo(id="test_repo1", pulp=pulp)
    repo2 = create_and_insert_repo(id="test_repo2", pulp=pulp)
    modulelist = [Module("perl-YAML", "1.24")]
    in_pulp_repos = [repo1, repo2]
    mod_dep_item = ModularDepsolverItem(modulelist, repo1, in_pulp_repos)

    return ModularDepsolver([mod_dep_item])
