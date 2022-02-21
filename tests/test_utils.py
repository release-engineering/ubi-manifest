from ubi_manifest.worker.tasks.depsolver.utils import (
    vercmp_sort,
    _keep_n_latest_rpms,
    flatten_list_of_sets,
    parse_bool_deps,
    get_n_latest_from_content,
)
from ubi_manifest.worker.tasks.depsolver.models import UbiUnit
from pubtools.pulplib import RpmUnit
import pytest


def get_ubi_unit(klass, repo_id, **kwargs):
    pulp_unit = klass(**kwargs)
    return UbiUnit(pulp_unit, repo_id)


def test_vercmp_sort():
    """Tests all comparison methods for vercmp sort used for RPM packages comparison"""
    vercmp_klass = vercmp_sort()

    unit_1 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="test",
        version="10",
        release="20",
        epoch="1",
        arch="x86_64",
    )

    unit_2 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="test",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
    )

    unit_1 = vercmp_klass(unit_1)
    unit_2 = vercmp_klass(unit_2)

    assert (unit_1 < unit_2) is True
    assert (unit_1 <= unit_2) is True
    assert (unit_1 == unit_2) is False
    assert (unit_1 >= unit_2) is False
    assert (unit_1 > unit_2) is False
    assert (unit_1 != unit_2) is True


def test_keep_n_latest_rpms():
    """Test keeping only the latest version of rpm"""
    unit_1 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="test",
        version="10",
        release="20",
        arch="x86_64",
    )

    unit_2 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="test",
        version="11",
        release="20",
        arch="x86_64",
    )

    rpms = [unit_1, unit_2]
    rpms.sort(key=vercmp_sort())
    _keep_n_latest_rpms(rpms)

    # there should only one rpm
    assert len(rpms) == 1
    # with the highest number of version
    assert rpms[0].version == "11"


def test_keep_n_latest_rpms_multiple_arches():
    """Test keeping only the latest version of rpm for multiple arches"""

    unit_1 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="test",
        version="10",
        release="20",
        arch="x86_64",
    )
    unit_2 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="test",
        version="11",
        release="20",
        arch="x86_64",
    )
    unit_3 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="test",
        version="10",
        release="20",
        arch="i686",
    )
    unit_4 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="test",
        version="9",
        release="20",
        arch="i686",
    )

    rpms = [unit_1, unit_2, unit_3, unit_4]
    rpms.sort(key=vercmp_sort())
    _keep_n_latest_rpms(rpms)

    # sort by version, the order after _keep_n_latest_rpms() is not guaranteed in this case
    rpms.sort(key=lambda x: x.version)

    # there should be 2 rpms
    assert len(rpms) == 2

    # i686 rpm goes with its highest version
    assert rpms[0].version == "10"
    assert rpms[0].arch == "i686"

    # x86_64 rpm goes with its highest version
    assert rpms[1].version == "11"
    assert rpms[1].arch == "x86_64"


def test_flatten_list_of_sets():
    """Test helper function that flattens list of sets into one set"""
    set_1 = set([1, 2, 3])
    set_2 = set([2, 3, 4])
    expected_set = set([1, 2, 3, 4])

    new_set = flatten_list_of_sets([set_1, set_2]).result()
    assert new_set == expected_set


def test_get_n_latest_from_content():
    unit_1 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="test",
        version="200",
        release="20",
        arch="x86_64",
    )
    unit_2 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="test",
        version="10",
        release="20",
        arch="x86_64",
    )
    unit_3 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="foo",
        version="100",
        release="20",
        arch="x86_64",
    )
    unit_4 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="foo",
        version="10",
        release="20",
        arch="x86_64",
    )

    units = [unit_1, unit_2, unit_3, unit_4]

    result = get_n_latest_from_content(units)
    result.sort(key=lambda x: x.name)

    assert len(result) == 2

    unit = result[0]
    assert unit.name == "foo"
    assert unit.version == "100"

    unit = result[1]
    assert unit.name == "test"
    assert unit.version == "200"


def test_get_n_latest_from_content_skip_modular_rpms():
    # non-modular unit
    unit_1 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="test",
        version="10",
        release="20",
        arch="x86_64",
        filename="test-10-20.x86_64.rpm",
    )

    # modular unit
    unit_2 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="test",
        version="100",
        release="20",
        arch="x86_64",
        filename="test-100-20.x86_64.rpm",
    )

    modular_rpms = "test-100-20.x86_64.rpm"
    units = [unit_1, unit_2]

    result = get_n_latest_from_content(units, modular_rpms)

    assert len(result) == 1

    unit = result[0]
    assert unit.name == "test"
    assert unit.version == "10"


@pytest.mark.parametrize(
    "clause, result",
    [
        # test data from https://rpm-software-management.github.io/rpm/manual/boolean_dependencies.html
        ("(pkgA and pkgB)", {"pkgA", "pkgB"}),
        ("(pkgA >= 3.2 or pkgB)", {"pkgA", "pkgB"}),
        ("(myPkg-langCZ if langsupportCZ)", {"myPkg-langCZ", "langsupportCZ"}),
        (
            "(myPkg-backend-mariaDB if mariaDB else sqlite)",
            {"myPkg-backend-mariaDB", "mariaDB", "sqlite"},
        ),
        ("(pkgA-foo with pkgA-bar)", {"pkgA-foo", "pkgA-bar"}),
        ("(pkgA-foo without pkgA-bar)", {"pkgA-foo", "pkgA-bar"}),
        ("(myPkg-driverA unless driverB)", {"myPkg-driverA", "driverB"}),
        (
            "(myPkg-backend-SDL1 unless myPkg-backend-SDL2 else SDL2)",
            {"myPkg-backend-SDL1", "myPkg-backend-SDL2", "SDL2"},
        ),
        ("(pkgA or pkgB or pkgC)", {"pkgA", "pkgB", "pkgC"}),
        ("(pkgA or (pkgB and pkgC))", {"pkgA", "pkgB", "pkgC"}),
        (
            "(foo and (lang-support-cz or lang-support-all))",
            {"foo", "lang-support-cz", "lang-support-all"},
        ),
        ("((pkgA with capB) or (pkgB without capA))", {"pkgA", "capB", "pkgB", "capA"}),
        (
            "((driverA and driverA-tools) unless driverB)",
            {"driverA", "driverA-tools", "driverB"},
        ),
        (
            "((myPkg-langCZ and (font1-langCZ or font2-langCZ)) if langsupportCZ)",
            {"myPkg-langCZ", "font1-langCZ", "font2-langCZ", "langsupportCZ"},
        ),
        # extra case for num operators
        (
            "(pkgA > 1.9 or pkgB >= 2 or pkgC = 4.1 or pkgD <= 9.6 and pkgE < 10.4)",
            {"pkgA", "pkgB", "pkgC", "pkgD", "pkgE"},
        ),
        # extra case for multiple appearance of the same package
        ("(pkgA < 1 or (pkgA >= 1 or pkgB) or pkgB > 2.4)", {"pkgA", "pkgB"}),
    ],
)
def test_parse_bool_deps(clause, result):
    parsed = parse_bool_deps(clause)
    assert parsed == result
