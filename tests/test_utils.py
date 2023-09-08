from unittest import mock

import pytest
from pubtools.pulplib import (
    Criteria,
    Matcher,
    ModulemdDependency,
    ModulemdUnit,
    RpmDependency,
    RpmUnit,
)

from ubi_manifest.worker.tasks.depsolver.models import PackageToExclude, UbiUnit
from ubi_manifest.worker.tasks.depsolver.ubi_config import UbiConfigLoader
from ubi_manifest.worker.tasks.depsolver.utils import (
    _keep_n_latest_rpms,
    create_or_criteria,
    flatten_list_of_sets,
    get_criteria_for_modules,
    get_modulemd_output_set,
    get_n_latest_from_content,
    is_requirement_resolved,
    parse_blacklist_config,
    parse_bool_deps,
    split_filename,
    vercmp_sort,
)

from .utils import MockLoader, rpmdeps_from_names


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

    new_set = flatten_list_of_sets([set_1, set_2])
    assert new_set == expected_set


def test_get_n_latest_from_content():
    """test function that takes rpms and returns onyl the latest version of them"""
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
    unit_5 = get_ubi_unit(
        RpmUnit,
        "test_repo_id",
        name="pkg_exclude_foo",
        version="10",
        release="20",
        arch="x86_64",
    )

    units = [unit_1, unit_2, unit_3, unit_4, unit_5]
    blacklist = [PackageToExclude("pkg_exclude", True, "x86_64")]

    result = get_n_latest_from_content(units, blacklist)
    result.sort(key=lambda x: x.name)

    # there should be only 2 units in the result
    assert len(result) == 2

    # units in the results have the highest version
    unit = result[0]
    assert unit.name == "foo"
    assert unit.version == "100"

    unit = result[1]
    assert unit.name == "test"
    assert unit.version == "200"


def test_get_n_latest_from_content_skip_modular_rpms():
    """test getting latest rpms while skipping modular rpms"""
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

    result = get_n_latest_from_content(units, [], modular_rpms)
    # there should be only one rpm, modular one is skipped
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
        # extra case #2 with nesting and valid paranthesis in dep.
        (
            "((pkgA(xxx) >= 0.1.2 with capB) or (pkgB <= 3.4.5 without capA))",
            {"pkgA(xxx)", "capB", "pkgB", "capA"},
        ),
        # case with extra spaces and parentheses
        (
            "(    ((( pkgA(xxx) >= 0.1.2 with capA    )))     )",
            {"pkgA(xxx)", "capA"},
        ),
    ],
)
def test_parse_bool_deps(clause, result):
    """
    test parsing bool/rich dependencies, the function extracts only names of packages
    """
    parsed = parse_bool_deps(clause)
    assert parsed == rpmdeps_from_names(*result)


def test_create_or_criteria():
    """Test creation of criteria list"""
    fields = ["color", "size"]
    values = [("blue", "10"), ("white", "15")]

    criteria = create_or_criteria(fields, values)

    # there should be 2 criteria created
    assert len(criteria) == 2
    # both of instance of Criteria
    for crit in criteria:
        assert isinstance(crit, Criteria)
    # let's not test internal structure of criteria, that's responsibility of pulplib


def test_create_or_criteria_uneven_args():
    """Test wrong number of values in args"""

    fields = ["color", "size"]
    values = [("blue", "10"), ("white")]
    # call to _create_or_criteria raises ValueError because of uneven number of values of the second tuple
    # in value list
    with pytest.raises(ValueError):
        _ = create_or_criteria(fields, values)


@pytest.mark.parametrize(
    "filename, name, ver, rel, epoch, arch",
    [
        (
            "32:bind-9.10.2-2.P1.fc22.x86_64.rpm",
            "bind",
            "9.10.2",
            "2.P1.fc22",
            "32",
            "x86_64",
        ),
        (
            "bind-9.10.2-2.P1.fc22.x86_64.rpm",
            "bind",
            "9.10.2",
            "2.P1.fc22",
            "",
            "x86_64",
        ),
    ],
)
def test_split_filename(filename, name, ver, rel, epoch, arch):
    result = split_filename(filename)

    assert result[0] == name
    assert result[1] == ver
    assert result[2] == rel
    assert result[3] == epoch
    assert result[4] == arch


def test_parse_blacklist():
    with mock.patch("ubiconfig.get_loader", return_value=MockLoader()):
        loader = UbiConfigLoader("https://foo.bar.com/some-repo.git")
        config = loader.get_config("cs_rpm_in", "cs_rpm_out", "8")

        parsed = parse_blacklist_config(config)

        assert len(parsed) == 3

        parsed = sorted(parsed, key=lambda x: x.name)

        item = parsed[0]
        assert item.name == "kernel"
        assert item.globbing is False
        assert item.arch is None

        item = parsed[1]
        assert item.name == "kernel"
        assert item.globbing is False
        assert item.arch == "x86_64"

        item = parsed[2]
        assert item.name == "package-name"
        assert item.globbing is True
        assert item.arch is None


def test_get_modulemd_output_set():
    # Define mock UbiUnits

    # two perl-YAML units with different versions, keep the highest version one
    module1 = ModulemdUnit(
        name="perl-YAML",
        stream="1.24",
        version=8,
        context="b7fad3bf",
        arch="x86_64",
    )
    unit1 = UbiUnit(module1, "test_repo1")

    module2 = ModulemdUnit(
        name="perl-YAML",
        stream="1.24",
        version=9,
        context="b7fad3bf",
        arch="x86_64",
    )
    unit2 = UbiUnit(module2, "test_repo1")

    # two perl units with different contexts and one with lower version
    # Both the units with the highest versions should be kept
    module3 = ModulemdUnit(
        name="perl",
        stream="5.30",
        version=8,
        context="abc",
        arch="x86_64",
    )
    unit3 = UbiUnit(module3, "test_repo1")

    module4 = ModulemdUnit(
        name="perl",
        stream="5.30",
        version=8,
        context="def",
        arch="x86_64",
    )
    unit4 = UbiUnit(module4, "test_repo1")

    module5 = ModulemdUnit(
        name="perl",
        stream="5.30",
        version=3,
        context="ABC",
        arch="x86_64",
    )
    unit5 = UbiUnit(module5, "test_repo1")

    expected_output_set = [module2, module3, module4]
    output_set = get_modulemd_output_set([module1, module2, module3, module4, module5])
    assert output_set == expected_output_set


def test_get_criteria_for_modules():
    # define units to search
    unit1 = ModulemdDependency(
        name="perl",
        stream="5.30",
    )

    unit2 = ModulemdDependency(
        name="perl",
        stream="6.30",
    )

    unit3 = ModulemdDependency(
        name="perl-YAML",
    )

    expected_criteria = create_or_criteria(
        ("name", "stream"),
        [("perl", "5.30"), ("perl", "6.30"), ("perl-YAML", Matcher.exists())],
    )
    criteria = get_criteria_for_modules([unit1, unit2, unit3])


@pytest.mark.parametrize(
    "requirement, provider, expected_result",
    [
        # no flags
        (RpmDependency(name="test-dep"), RpmDependency(name="test-dep"), True),
        (RpmDependency(name="test-dep"), RpmDependency(name="test-dep-other"), False),
        # flag GT - greater than
        (
            RpmDependency(
                name="test-dep", version="9", release="el10", epoch="0", flags="GT"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            True,
        ),
        (
            RpmDependency(
                name="test-dep", version="10", release="el10", epoch="0", flags="GT"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            False,
        ),
        (
            RpmDependency(
                name="test-dep", version="11", release="el10", epoch="0", flags="GT"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            False,
        ),
        # flag GE - greater or equal
        (
            RpmDependency(
                name="test-dep", version="9", release="el10", epoch="0", flags="GE"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            True,
        ),
        (
            RpmDependency(
                name="test-dep", version="10", release="el10", epoch="0", flags="GE"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            True,
        ),
        (
            RpmDependency(
                name="test-dep", version="11", release="el10", epoch="0", flags="GE"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            False,
        ),
        # flag EQ - equal
        (
            RpmDependency(
                name="test-dep", version="9", release="el10", epoch="0", flags="EQ"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            False,
        ),
        (
            RpmDependency(
                name="test-dep", version="10", release="el10", epoch="0", flags="EQ"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            True,
        ),
        (
            RpmDependency(
                name="test-dep", version="11", release="el10", epoch="0", flags="EQ"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            False,
        ),
        # flag LE - less or equal
        (
            RpmDependency(
                name="test-dep", version="9", release="el10", epoch="0", flags="LE"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            False,
        ),
        (
            RpmDependency(
                name="test-dep", version="10", release="el10", epoch="0", flags="LE"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            True,
        ),
        (
            RpmDependency(
                name="test-dep", version="11", release="el10", epoch="0", flags="LE"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            True,
        ),
        # flag LT - less than
        (
            RpmDependency(
                name="test-dep", version="9", release="el10", epoch="0", flags="LT"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            False,
        ),
        (
            RpmDependency(
                name="test-dep", version="10", release="el10", epoch="0", flags="LT"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            False,
        ),
        (
            RpmDependency(
                name="test-dep", version="11", release="el10", epoch="0", flags="LT"
            ),
            RpmDependency(name="test-dep", version="10", release="el10", epoch="0"),
            True,
        ),
    ],
)
def test_is_requirement_resolved(requirement, provider, expected_result):
    resolved = is_requirement_resolved(requirement, provider)
    assert resolved is expected_result
