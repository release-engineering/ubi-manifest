from unittest import mock

from pubtools.pulplib import Distributor, RpmUnit

from ubi_manifest.worker.tasks import depsolve

from .utils import MockLoader, create_and_insert_repo


def test_depsolve_task(pulp):
    """
    Simulate run of depsolve task, check expected output of depsolving.
    TODO task will return None in future, this test needs to be fixed accordingly
    """
    ubi_repo = create_and_insert_repo(
        id="ubi_repo",
        pulp=pulp,
        population_sources=["rhel_repo"],
        relative_url="foo/bar/os",
        ubi_config_version="8.4",
        content_set="rpm_in",
    )
    rhel_repo = create_and_insert_repo(id="rhel_repo", pulp=pulp)

    distributor_debug = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="ubi_debug_repo",
        relative_url="foo/bar/debug",
    )

    ubi_debug_repo = create_and_insert_repo(
        id="ubi_debug_repo",
        pulp=pulp,
        population_sources=["rhel_debug_repo"],
        relative_url="foo/bar/debug",
        distributors=[distributor_debug],
    )
    rhel_debug_repo = create_and_insert_repo(id="rhel_debug_repo", pulp=pulp)

    distributor_source = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="ubi_source_repo",
        relative_url="foo/bar/source/SRPMS",
    )

    ubi_source_repo = create_and_insert_repo(
        id="ubi_source_repo",
        pulp=pulp,
        population_sources=["rhel_source_repo"],
        relative_url="foo/bar/source/SRPMS",
        distributors=[distributor_source],
    )
    rhel_source_repo = create_and_insert_repo(id="rhel_source_repo", pulp=pulp)

    unit_binary = RpmUnit(
        name="gcc",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        sourcerpm="gcc_src-1-0.src.rpm",
        requires=[],
        provides=[],
    )

    unit_debuginfo = RpmUnit(
        name="gcc-debuginfo",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
    )
    unit_debugsource = RpmUnit(
        name="gcc_src-debugsource",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
    )
    unit_srpm = RpmUnit(
        name="gcc_src",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="gcc_src-1-0.src.rpm",
        content_type_id="srpm",
    )

    pulp.insert_units(rhel_repo, [unit_binary])
    pulp.insert_units(rhel_debug_repo, [unit_debuginfo, unit_debugsource])
    pulp.insert_units(rhel_source_repo, [unit_srpm])

    with mock.patch("ubi_manifest.worker.tasks.depsolver.utils.Client") as client:
        with mock.patch("ubiconfig.get_loader", return_value=MockLoader()):
            client.return_value = pulp.client
            # let run the depsolve task
            result = depsolve.depsolve_task(["ubi_repo"])

            # there should be 2 repos in output - one binary and one debuginfo
            assert sorted(list(result.keys())) == [
                "ubi_debug_repo",
                "ubi_repo",
                "ubi_source_repo",
            ]

            # binary repo contains only one rpm
            content = result["ubi_repo"]
            assert len(content) == 1
            unit = content[0]
            assert unit.name == "gcc"

            # debuginfo repo conains two debug packages
            content = sorted(result["ubi_debug_repo"], key=lambda x: x.name)
            assert len(content) == 2
            unit = content[0]
            assert unit.name == "gcc-debuginfo"
            unit = content[1]
            assert unit.name == "gcc_src-debugsource"

            # source repo contain one SRPM package
            content = result["ubi_source_repo"]
            assert len(content) == 1
            unit = content[0]
            assert unit.name == "gcc_src"
            assert unit.content_type_id == "srpm"
