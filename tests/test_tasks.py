import json
from unittest import mock

from pubtools.pulplib import Distributor, RpmUnit

from ubi_manifest.worker.tasks import depsolve

from .utils import MockedRedis, MockLoader, create_and_insert_repo


def test_depsolve_task(pulp):
    """
    Simulate run of depsolve task, check expected output of depsolving.
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
        filename="gcc-10.200.x86_64.rpm",
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
        filename="gcc-debuginfo-10.200.x86_64.rpm",
    )
    unit_debugsource = RpmUnit(
        name="gcc_src-debugsource",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="gcc_src-debugsource-10.200.x86_64.rpm",
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
            with mock.patch(
                "ubi_manifest.worker.tasks.depsolve.redis.from_url"
            ) as mock_redis_from_url:
                redis = MockedRedis(data={})
                mock_redis_from_url.return_value = redis

                client.return_value = pulp.client
                # let run the depsolve task
                result = depsolve.depsolve_task(["ubi_repo"])
                # we don't return anything useful, everything is saved in redis
                assert result is None

                # there should 3 keys stored in redis
                assert sorted(redis.keys()) == [
                    "manifest:ubi_debug_repo",
                    "manifest:ubi_repo",
                    "manifest:ubi_source_repo",
                ]

                # load json string stored in redis
                data = redis.get("manifest:ubi_repo")
                content = json.loads(data)
                # binary repo contains only one rpm
                assert len(content) == 1
                unit = content[0]
                assert unit["src_repo_id"] == "rhel_repo"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "gcc-10.200.x86_64.rpm"

                # load json string stored in redis
                data = redis.get("manifest:ubi_debug_repo")
                content = sorted(json.loads(data), key=lambda d: d["value"])
                # debuginfo repo conains two debug packages
                assert len(content) == 2
                unit = content[0]
                assert unit["src_repo_id"] == "rhel_debug_repo"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "gcc-debuginfo-10.200.x86_64.rpm"

                unit = content[1]
                assert unit["src_repo_id"] == "rhel_debug_repo"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "gcc_src-debugsource-10.200.x86_64.rpm"

                # load json string stored in redis
                data = redis.get("manifest:ubi_source_repo")
                content = json.loads(data)
                # source repo contain one SRPM package
                assert len(content) == 1
                unit = content[0]
                assert unit["src_repo_id"] == "rhel_source_repo"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "gcc_src-1-0.src.rpm"
