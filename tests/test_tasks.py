from unittest import mock

from pubtools.pulplib import RpmUnit

from ubi_manifest.worker.tasks import depsolve

from .utils import MockLoader, create_and_insert_repo


def test_depsolve_task(pulp):
    with mock.patch(
        "pubtools.pulplib.YumRepository.get_debug_repository"
    ) as get_debug_mock:
        ubi_repo = create_and_insert_repo(
            id="ubi_repo",
            pulp=pulp,
            population_sources=["rhel_repo"],
            relative_url="foo/bar/os",
            ubi_config_version="8.4",
            content_set="rpm_in",
        )
        rhel_repo = create_and_insert_repo(id="rhel_repo", pulp=pulp)

        ubi_debug_repo = create_and_insert_repo(
            id="ubi_debug_repo",
            pulp=pulp,
            population_sources=["rhel_debug_repo"],
            relative_url="foo/bar/debug",
        )
        rhel_debug_repo = create_and_insert_repo(id="rhel_debug_repo", pulp=pulp)

        get_debug_mock.return_value = ubi_debug_repo

        ubi_repo.get_debug_repository = mock.MagicMock()
        ubi_repo.get_debug_repository.return_value = ubi_debug_repo

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

        pulp.insert_units(rhel_repo, [unit_binary])
        pulp.insert_units(rhel_debug_repo, [unit_debuginfo, unit_debugsource])

        with mock.patch("ubi_manifest.worker.tasks.depsolver.utils.Client") as client:
            with mock.patch("ubiconfig.get_loader", return_value=MockLoader()):
                client.return_value = pulp.client
                # let run the depsolve task
                result = depsolve.depsolve_task(["ubi_repo"])

                # there should be 2 repos in output - one binary and one debuginfo
                assert sorted(list(result.keys())) == ["ubi_debug_repo", "ubi_repo"]

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
