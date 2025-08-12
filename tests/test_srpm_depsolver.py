from pubtools.pulplib import Distributor, RpmUnit

from ubi_manifest.worker.models import PackageToExclude
from ubi_manifest.worker.tasks.depsolver import SrpmDepsolver

from .utils import create_and_insert_repo


def _prepare_test_data(pulp):
    dist_sr1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="test_repo_srpm_1",
        relative_url="/location/repo_1/source/SRPMS",
    )
    dist_sr2 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="test_repo_srpm_1",
        relative_url="/location/repo_2/source/SRPMS",
    )
    repo_srpm = create_and_insert_repo(
        id=dist_sr1.repo_id,
        pulp=pulp,
        relative_url=dist_sr2.relative_url,
        distributors=[dist_sr1, dist_sr2],
    )

    other_dist_sr1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="test_repo_srpm_2",
        relative_url="/location/other_repo_1/source/SRPMS",
    )
    other_dist_sr2 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="test_repo_srpm_2",
        relative_url="/location/other_repo_2/source/SRPMS",
    )
    other_repo_srpm = create_and_insert_repo(
        id=other_dist_sr1.repo_id,
        pulp=pulp,
        relative_url=other_dist_sr1.relative_url,
        distributors=[other_dist_sr1, other_dist_sr2],
    )

    unit_1 = RpmUnit(
        name="gcc",
        filename="gcc.src.rpm",
        version="100",
        release="200",
        epoch="1",
        arch="x86_64",
        provides=[],
        requires=[],
        content_type_id="srpm",
    )

    unit_2 = RpmUnit(
        name="lib-y-source",
        filename="lib-y.src.rpm",
        version="100",
        release="200",
        epoch="1",
        arch="x86_64",
        provides=[],
        requires=[],
        content_type_id="srpm",
    )

    unit_3 = RpmUnit(
        name="blacklisted",
        filename="blacklisted.src.rpm",
        version="100",
        release="200",
        epoch="1",
        arch="x86_64",
        provides=[],
        requires=[],
        content_type_id="srpm",
    )

    unit_4 = RpmUnit(
        name="other-blacklisted",
        filename="blacklisted.src.rpm",
        version="100",
        release="200",
        epoch="1",
        arch="x86_64",
        provides=[],
        requires=[],
        content_type_id="srpm",
    )

    repo_srpm_units = [unit_1, unit_2, unit_3]
    pulp.insert_units(repo_srpm, repo_srpm_units)
    other_repo_srpm_units = [unit_1, unit_4]
    pulp.insert_units(other_repo_srpm, other_repo_srpm_units)

    blacklist = ["blacklisted", "other-blacklisted"]

    expected_output_set = [
        (unit.name, "test_repo_srpm_1")
        for unit in repo_srpm_units
        if unit.name not in blacklist
    ] + [
        (unit.name, "test_repo_srpm_2")
        for unit in other_repo_srpm_units
        if unit.name not in blacklist
    ]

    return [repo_srpm, other_repo_srpm], sorted(expected_output_set)


def test_run(pulp):
    """test the main method of srpm depsolver"""
    source_repos, expected_output_set = _prepare_test_data(pulp)

    blacklist_1 = [PackageToExclude("blacklisted")]
    blacklist_2 = [PackageToExclude("other-blacklisted")]

    srpm_filenames = {
        "test_repo_srpm_1": ["gcc.src.rpm", "lib-y.src.rpm", "blacklisted"],
        "test_repo_srpm_2": ["gcc.src.rpm", "other-blacklisted"],
    }

    with SrpmDepsolver(
        srpm_filenames, source_repos, [blacklist_1, blacklist_2]
    ) as depsolver:
        depsolver.run()
        output = [
            (item.name, item.associate_source_repo_id)
            for item in depsolver.srpm_output_set
        ]
        assert sorted(output) == expected_output_set
