from importlib.metadata import requires
from pubtools.pulplib import YumRepository, RpmUnit, RpmDependency, ModulemdUnit
from ubi_manifest.worker.tasks.depsolver.rpm_depsolver import Depsolver
from ubi_manifest.worker.tasks.depsolver.models import UbiRepository


def get_test_yum_repository(**kwargs):
    pulp = kwargs.pop("pulp")
    repo = YumRepository(**kwargs)
    repo.__dict__["_client"] = pulp.client
    return repo


def test_what_provides(pulp):
    depsolver = Depsolver(None)

    requires = ["gcc"]

    repo = get_test_yum_repository(id="test_repo_id", pulp=pulp)

    unit_1 = RpmUnit(
        name="test",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        provides=[RpmDependency(name="gcc")],
    )

    unit_2 = RpmUnit(
        name="test",
        version="100",
        release="200",
        epoch="1",
        arch="x86_64",
        provides=[RpmDependency(name="gcc")],
    )

    pulp.insert_repository(repo)
    pulp.insert_units(repo, [unit_1, unit_2])

    result = depsolver.what_provides(requires, [repo])

    assert len(result) == 1
    unit = result[0]
    assert unit.version == "100"
    assert unit.provides[0].name == "gcc"


def test_extract_and_resolve():
    depsolver = Depsolver(None)

    # set some data to depsolver instance

    depsolver._requires = {"pkg_a", "pkg_b"}
    depsolver._provides = {"pkg_c", "pkg_d"}
    depsolver._unsolved = {"pkg_a", "pkg_b"}

    # udelat ubiunitu
    unit = RpmUnit(
        name="test",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        provides=[RpmDependency(name="pkg_e"), RpmDependency(name="pkg_b")],
        requires=[RpmDependency(name="pkg_f")],
    )

    depsolver.extract_and_resolve([unit])

    # internal state of depsolver should change
    # pkg_f is a new requirement that is added to the requires set
    assert depsolver._requires == {"pkg_a", "pkg_b", "pkg_f"}
    # pkg_e and pkg_b are added to the provides set
    assert depsolver._provides == {"pkg_c", "pkg_d", "pkg_e", "pkg_b"}
    # pkg_b is resolved but pkg_f is added as new unsolved requirement
    assert depsolver._unsolved == {"pkg_a", "pkg_f"}


### test na bool deps


def test_get_base_packages(pulp):
    depsolver = Depsolver(None)

    repo = get_test_yum_repository(id="test_repo_id", pulp=pulp)

    unit_1 = RpmUnit(
        name="test",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
    )

    unit_2 = RpmUnit(
        name="test",
        version="100",
        release="200",
        epoch="1",
        arch="x86_64",
    )

    pulp.insert_repository(repo)
    pulp.insert_units(repo, [unit_1, unit_2])

    pkgs_to_search = ["test"]

    result = depsolver.get_base_packages(repo, pkgs_to_search)

    assert len(result) == 1
    unit = result[0]
    assert unit.name == "test"
    assert unit.version == "100"


def test_get_pkgs_from_all_modules(pulp):
    """tests getting pkgs filenames from all available modulemd units"""
    depsolver = Depsolver(None)

    repo = get_test_yum_repository(id="test_repo_1", pulp=pulp)

    unit_1 = ModulemdUnit(
        name="test",
        stream="10",
        version=100,
        context="abcdef",
        arch="x86_64",
        artifacts=[
            "perl-version-7:0.99.24-441.module+el8.3.0+6718+7f269185.src",
            "perl-version-7:0.99.24-441.module+el8.3.0+6718+7f269185.x86_64",
        ],
    )
    unit_2 = ModulemdUnit(
        name="test",
        stream="20",
        version=100,
        context="abcdef",
        arch="x86_64",
        artifacts=[
            "perl-version-7:1.99.24-441.module+el8.4.0+9911+7f269185.src",
            "perl-version-7:1.99.24-441.module+el8.4.0+9911+7f269185.x86_64",
        ],
    )

    pulp.insert_repository(repo)
    pulp.insert_units(repo, [unit_1, unit_2])

    ft = depsolver._get_pkgs_from_all_modules([repo])

    result = ft.result()

    # there are 4 filenames according from 2 modulemd units
    expected_filenames = set(
        [
            "perl-version-0.99.24-441.module+el8.3.0+6718+7f269185.src.rpm",
            "perl-version-0.99.24-441.module+el8.3.0+6718+7f269185.x86_64.rpm",
            "perl-version-1.99.24-441.module+el8.4.0+9911+7f269185.src.rpm",
            "perl-version-1.99.24-441.module+el8.4.0+9911+7f269185.x86_64.rpm",
        ]
    )

    assert len(result) == 4
    assert result == expected_filenames


def test_run(pulp):

    repos = _prepare_pulp_data(pulp)

    whitelist_1 = ["gcc", "lib.a"]
    ubi_repo_1 = UbiRepository(
        whitelist=whitelist_1,
        in_pulp_repo=repos[0],
        out_pulp_repo=None,
        resolved=None,
    )

    whitelist_2 = ["jq"]
    ubi_repo_2 = UbiRepository(
        whitelist=whitelist_2,
        in_pulp_repo=repos[1],
        out_pulp_repo=None,
        resolved=None,
    )

    depsolver = Depsolver([ubi_repo_1, ubi_repo_2])
    depsolver.run()

    ### check itnernal state of depsolver object
    # provides set holds all capabilities that we went through during depsolving
    assert depsolver._provides == {}

    # requires set holds all requires that we went through during depsolving
    assert depsolver._requires == {}

    # unsolved set should be empty after depsolving finishes
    assert len(depsolver._unsolved) == 0

    # there can be unsolved requires, we can get those by
    unsolved = depsolver._requires - depsolver._provides
    assert unsolved == {}


# helper methods for test_run()
def _prepare_pulp_data(pulp):
    repo_1 = get_test_yum_repository(id="test_repo_1", pulp=pulp)

    repo_2 = get_test_yum_repository(id="test_repo_2", pulp=pulp)

    unit_1 = RpmUnit(
        name="gcc",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        provides=[RpmDependency(name="gcc"), RpmDependency(name="lib.a")],
        requires=[RpmDependency(name="lib.b")],
    )

    unit_2 = RpmUnit(
        name="jq",
        version="100",
        release="200",
        epoch="1",
        arch="x86_64",
        provides=[RpmDependency(name="jq")],
        requires=[RpmDependency(name="lib.c"), RpmDependency(name="lib.d")],
    )

    unit_3 = RpmUnit(
        name="foo",
        version="100",
        release="200",
        epoch="1",
        arch="x86_64",
        provides=[RpmDependency(name="foo"), RpmDependency(name="lib.b")],
        requires=[RpmDependency(name="lib.a"), RpmDependency(name="lib.b")],
    )

    pulp.insert_repository(repo_1)
    pulp.insert_repository(repo_2)

    pulp.insert_units(repo_1, [unit_1, unit_2])

    repos = []
    return repos
