import pytest
from pubtools.pulplib import RpmUnit

from ubi_manifest.worker.models import UbiUnit


def test_ubi_unit():
    """Test proper wrapping *Unit classes of pulplib and access of their attrs"""
    unit = RpmUnit(name="test", version="1.0", release="1", arch="x86_64")

    repo_id = "test_repo_id"
    ubi_unit = UbiUnit(unit, repo_id)

    # we can directly access attrs of RpmUnit
    assert ubi_unit.name == "test"
    assert ubi_unit.version == "1.0"
    assert ubi_unit.release == "1"
    assert ubi_unit.arch == "x86_64"
    assert ubi_unit.associate_source_repo_id == repo_id
    assert str(ubi_unit) == str(unit)

    # non-existing attr will raise an error
    with pytest.raises(AttributeError):
        _ = ubi_unit.non_existing_attr


def test_ubi_unit_bad_eq():
    unit = RpmUnit(name="test", version="1.0", release="1", arch="x86_64")

    repo_id = "test_repo_id"
    ubi_unit = UbiUnit(unit, repo_id)

    assert ubi_unit.__eq__(object()) == NotImplemented
