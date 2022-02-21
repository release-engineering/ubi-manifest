import pytest
from ubi_manifest.worker.tasks.depsolver.pulp_queries import (
    make_pulp_client,
    search_rpms,
    search_modulemds,
    _search_units_per_repos,
    _search_units,
)
from ubi_manifest.worker.tasks.depsolver.utils import _create_or_criteria

from pubtools.pulplib import Client, YumRepository, RpmUnit
from attrs import define
import pytest
from pubtools.pulplib import RpmUnit, ModulemdUnit, ModulemdDefaultsUnit, FakeController

from ubi_manifest.worker.tasks.depsolver.models import UbiUnit
import requests_mock

from pubtools.pulplib import YumRepository


def get_test_yum_repository(**kwargs):
    pulp = kwargs.pop("pulp")
    repo = YumRepository(**kwargs)
    repo.__dict__["_client"] = pulp.client
    return repo


@pytest.fixture
def requests_mocker():
    """Yields a new requests_mock Mocker.

    This is the same as using Mocker as a function decorator, but instead
    uses the pytest fixture system.
    """
    with requests_mock.Mocker() as mocker:
        yield mocker


@define
class TestConfig:
    url: str
    username: str
    password: str
    insecure: bool


def test_make_pulp_client():
    config = TestConfig(
        url="https://fake.pulp.com",
        username="test_user",
        password="test_pass",
        insecure=True,
    )

    client = make_pulp_client(config)

    # Client instance is prperly created with no errors
    assert isinstance(client, Client)


def test_search_rpms(pulp):
    """Test method for searching rpms"""
    repo = get_test_yum_repository(
        id="test_repo_1",
        pulp=pulp,
    )

    unit_1 = RpmUnit(
        name="test",
        version="1.0",
        release="1",
        arch="x86_64",
        filename="test.x86_64.rpm",
    )
    unit_2 = RpmUnit(
        name="test", version="1.0", release="1", arch="i386", filename="test.i386.rpm"
    )

    pulp.insert_repository(repo)
    pulp.insert_units(repo, [unit_1, unit_2])

    criteria = _create_or_criteria(["filename"], [("test.x86_64.rpm",)])

    # let Future return result
    result = search_rpms(criteria, [repo]).result()
    # there should be be only one unit in the result set according to criteria
    assert len(result) == 1
    unit = result.pop()
    assert unit.filename == "test.x86_64.rpm"
    assert isinstance(unit, UbiUnit)
    assert isinstance(unit._unit, RpmUnit)


def test_search_modulemds(pulp):
    """Test convenient method for searching modulemds"""
    repo = get_test_yum_repository(
        id="test_repo_1",
        pulp=pulp,
    )
    unit_1 = ModulemdUnit(
        name="test",
        stream="10",
        version=100,
        context="abcdef",
        arch="x86_64",
    )
    unit_2 = ModulemdUnit(
        name="test",
        stream="20",
        version=100,
        context="abcdef",
        arch="x86_64",
    )

    pulp.insert_repository(repo)
    pulp.insert_units(repo, [unit_1, unit_2])

    criteria = _create_or_criteria(["name", "stream"], [("test", "10")])
    # let Future return result
    result = search_modulemds(criteria, [repo]).result()
    # there should be be only one unit in the result set according to criteria
    assert len(result) == 1
    unit = result.pop()

    assert unit.nsvca == "test:10:100:abcdef:x86_64"
    assert isinstance(unit, UbiUnit)
    assert isinstance(unit._unit, ModulemdUnit)


def test_search_units_per_repos(pulp):
    """Test searching over multiple repositories"""
    repo_1 = get_test_yum_repository(
        id="test_repo_1",
        pulp=pulp,
    )
    repo_2 = get_test_yum_repository(id="test_repo_2", pulp=pulp)

    unit_1 = RpmUnit(name="test", version="1.0", release="1", arch="x86_64")
    unit_2 = RpmUnit(name="test", version="1.0", release="1", arch="i386")

    pulp.insert_repository(repo_1)
    pulp.insert_repository(repo_2)
    pulp.insert_units(repo_1, [unit_1])
    pulp.insert_units(repo_2, [unit_2])

    expected_repo_ids = ["test_repo_1", "test_repo_2"]

    criteria = _create_or_criteria(
        ["name", "arch"], [("test", "x86_64"), ("test", "i386")]
    )

    # let Future return result
    search_result = _search_units_per_repos(
        criteria, [repo_1, repo_2], RpmUnit
    ).result()

    # result should be set
    assert isinstance(search_result, set)
    # with 2 items
    assert len(search_result) == 2
    # units are from both repos
    actual_repo_ids = []
    for unit in search_result:
        actual_repo_ids.append(unit.associate_source_repo_id)
        assert isinstance(unit, UbiUnit)
        assert isinstance(unit._unit, RpmUnit)
    assert sorted(actual_repo_ids) == expected_repo_ids


def test_search_units(pulp):
    """Test simple search for units"""
    repo = get_test_yum_repository(id="test_repo", pulp=pulp)
    unit_1 = RpmUnit(name="test", version="1.0", release="1", arch="x86_64")
    unit_2 = RpmUnit(name="test", version="1.0", release="1", arch="i386")
    pulp.insert_repository(repo)
    pulp.insert_units(repo, [unit_1, unit_2])

    criteria = _create_or_criteria(["name", "arch"], [("test", "x86_64")])
    # let Future return result
    search_result = _search_units(repo, criteria, RpmUnit).result()

    # result should be set
    assert isinstance(search_result, set)
    # with only 1 item
    assert len(search_result) == 1
    unit = search_result.pop()
    # unit should be UbiUnit
    assert isinstance(unit, UbiUnit)
    # internally _unit attr should be RpmUnit
    assert isinstance(unit._unit, RpmUnit)
    # unit has name "test"
    assert unit.name == "test"
    # and proper associate_source_repo_id set
    assert unit.associate_source_repo_id == "test_repo"


def test_search_units_handle_pages(pulp):
    """test proper handling of pagination"""
    repo = get_test_yum_repository(id="test_repo", pulp=pulp)

    units = []
    # let's use higher number of units, we don't want to rely
    # on fake pulp settings of pagination
    for num in range(200):
        unit = RpmUnit(name="test", version=str(num), release="1", arch="x86_64")
        units.append(unit)

    pulp.insert_repository(repo)
    pulp.insert_units(repo, units)

    criteria = _create_or_criteria(["name"], [("test",)])

    search_result = _search_units(repo, criteria, RpmUnit).result()
    # result should be set
    assert isinstance(search_result, set)
    # all units are retured
    assert len(search_result) == 200


def test_search_units_batch_split(pulp):
    repo = get_test_yum_repository(id="test_repo", pulp=pulp)
    unit_1 = RpmUnit(name="test-1", version="1.0", release="1", arch="x86_64")
    unit_2 = RpmUnit(name="test-2", version="1.0", release="1", arch="i386")
    unit_3 = RpmUnit(name="test-3", version="1.0", release="1", arch="s390x")

    pulp.insert_repository(repo)
    pulp.insert_units(repo, [unit_1, unit_2, unit_3])

    criteria = _create_or_criteria(["name"], [("test-1",), ("test-2",), ("test-3",)])
    # batch_size_override=1 should end with 3 queries to pulp
    search_result = _search_units(
        repo, criteria, RpmUnit, batch_size_override=1
    ).result()
    # result should be set
    assert isinstance(search_result, set)
    # 3 units are properly returned
    assert len(search_result) == 3
