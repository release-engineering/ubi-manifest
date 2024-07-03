import json
from functools import partial
from unittest import mock

import pytest
from pubtools.pulplib import Distributor, ModulemdDefaultsUnit, ModulemdUnit, RpmUnit

from ubi_manifest.worker.tasks import depsolve
from ubi_manifest.worker.ubi_config import ContentConfigMissing

from .utils import MockedRedis, MockLoader, create_and_insert_repo


def test_depsolve_task(pulp):
    """
    Simulate run of depsolve task, check expected output of depsolving this the basic scenario with only one input repository.
    """
    ubi_repo = create_and_insert_repo(
        id="ubi_repo",
        pulp=pulp,
        population_sources=["rhel_repo"],
        relative_url="foo/bar/os",
        ubi_config_version="8.4",
        content_set="cs_rpm_out",
    )
    distributor_rhel = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_repo",
        relative_url="foo/rhel/os",
    )
    rhel_repo = create_and_insert_repo(
        id="rhel_repo",
        pulp=pulp,
        content_set="cs_rpm_in",
        relative_url="foo/rhel/os",
        distributors=[distributor_rhel],
    )

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
        content_set="cs_debug_out",
    )

    distributor_rhel_debug = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_debug_repo",
        relative_url="foo/rhel/debug",
    )
    rhel_debug_repo = create_and_insert_repo(
        id="rhel_debug_repo",
        pulp=pulp,
        content_set="cs_debug_in",
        relative_url="foo/rhel/debug",
        distributors=[distributor_rhel_debug],
    )

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
        content_set="cs_srpm_out",
    )

    distributor_rhel_source = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_source_repo",
        relative_url="foo/rhel/source/SRPMS",
    )
    rhel_source_repo = create_and_insert_repo(
        id=distributor_rhel_source.repo_id,
        pulp=pulp,
        content_set="cs_srpm_in",
        relative_url=distributor_rhel_source.relative_url,
        distributors=[distributor_rhel_source],
    )

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
        sourcerpm="gcc_src-1-0.src.rpm",
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
        sourcerpm="gcc_src_debug-1-0.src.rpm",
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
    unit_srpm_debug = RpmUnit(
        name="gcc_src_debug",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="gcc_src_debug-1-0.src.rpm",
        content_type_id="srpm",
    )
    unit_modulemd = ModulemdUnit(
        name="fake_name",
        stream="fake_stream",
        version=8,
        context="b7fad3bf",
        arch="x86_64",
        artifacts=[
            "test-0:1.24-3.module+el8.1.0+2934+dec45db7.noarch",
            "test-0:1.24-3.module+el8.1.0+2934+dec45db7.src",
        ],
    )

    unit_modulemd_default = ModulemdDefaultsUnit(
        name="fake_name", stream="fake_stream", repo_id="rhel_repo"
    )

    pulp.insert_units(rhel_repo, [unit_binary, unit_modulemd, unit_modulemd_default])
    pulp.insert_units(rhel_repo, [unit_binary])
    pulp.insert_units(rhel_debug_repo, [unit_debuginfo, unit_debugsource])
    pulp.insert_units(rhel_source_repo, [unit_srpm, unit_srpm_debug])

    with mock.patch("ubi_manifest.worker.utils.Client") as client:
        with mock.patch("ubiconfig.get_loader", return_value=MockLoader()):
            with mock.patch(
                "ubi_manifest.worker.tasks.depsolve.redis.from_url"
            ) as mock_redis_from_url:
                redis = MockedRedis(data={})
                mock_redis_from_url.return_value = redis

                client.return_value = pulp.client
                # let run the depsolve task
                result = depsolve.depsolve_task(["ubi_repo"], "fake-url")
                # we don't return anything useful, everything is saved in redis
                assert result is None

                # there should 3 keys stored in redis
                assert sorted(redis.keys()) == [
                    "ubi_debug_repo",
                    "ubi_repo",
                    "ubi_source_repo",
                ]

                # load json string stored in redis
                data = redis.get("ubi_repo")
                content = json.loads(data)
                # binary repo contains only one rpm
                assert len(content) == 3
                unit = content[0]
                assert unit["src_repo_id"] == "rhel_repo"
                assert unit["unit_type"] == "ModulemdUnit"
                assert unit["unit_attr"] == "nsvca"
                assert unit["value"] == "fake_name:fake_stream:8:b7fad3bf:x86_64"
                unit = content[1]
                assert unit["src_repo_id"] == "rhel_repo"
                assert unit["unit_type"] == "ModulemdDefaultsUnit"
                assert unit["unit_attr"] == "name:stream"
                assert unit["value"] == "fake_name:fake_stream"
                unit = content[2]
                assert unit["src_repo_id"] == "rhel_repo"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "gcc-10.200.x86_64.rpm"

                # load json string stored in redis
                data = redis.get("ubi_debug_repo")
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
                data = redis.get("ubi_source_repo")
                content = json.loads(data)
                # source repo contain two SRPM packages, no duplicates
                assert len(content) == 2
                unit = content[0]
                assert unit["src_repo_id"] == "rhel_source_repo"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "gcc_src-1-0.src.rpm"

                unit = content[1]
                assert unit["src_repo_id"] == "rhel_source_repo"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "gcc_src_debug-1-0.src.rpm"


def test_depsolve_task_empty_manifests(pulp):
    """
    Simulate run of depsolve task, check expected output of depsolving. Extra case
    for empty manifests.
    """
    ubi_repo = create_and_insert_repo(
        id="ubi_repo",
        pulp=pulp,
        population_sources=["rhel_repo"],
        relative_url="foo/bar/os",
        ubi_config_version="8.4",
        content_set="cs_rpm_out",
    )
    distributor_rhel = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_repo",
        relative_url="foo/rhel/os",
    )
    rhel_repo = create_and_insert_repo(
        id="rhel_repo",
        pulp=pulp,
        content_set="cs_rpm_in",
        relative_url="foo/rhel/os",
        distributors=[distributor_rhel],
    )

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
        content_set="cs_debug_out",
    )

    distributor_rhel_debug = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_debug_repo",
        relative_url="foo/rhel/debug",
    )
    rhel_debug_repo = create_and_insert_repo(
        id="rhel_debug_repo",
        pulp=pulp,
        content_set="cs_debug_in",
        relative_url="foo/rhel/debug",
        distributors=[distributor_rhel_debug],
    )

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
        content_set="cs_srpm_out",
    )
    rhel_source_repo = create_and_insert_repo(
        id="rhel_source_repo", pulp=pulp, content_set="cs_srpm_in"
    )

    with mock.patch("ubi_manifest.worker.utils.Client") as client:
        with mock.patch("ubiconfig.get_loader", return_value=MockLoader()):
            with mock.patch(
                "ubi_manifest.worker.tasks.depsolve.redis.from_url"
            ) as mock_redis_from_url:
                redis = MockedRedis(data={})
                mock_redis_from_url.return_value = redis

                client.return_value = pulp.client
                # let run the depsolve task
                result = depsolve.depsolve_task(["ubi_repo"], "fake-url")
                # we don't return anything useful, everything is saved in redis
                assert result is None

                # there should 3 keys stored in redis
                assert sorted(redis.keys()) == [
                    "ubi_debug_repo",
                    "ubi_repo",
                    "ubi_source_repo",
                ]

                for repo in [
                    "ubi_debug_repo",
                    "ubi_repo",
                    "ubi_source_repo",
                ]:
                    # load json string stored in redis
                    data = redis.get(repo)
                    content = json.loads(data)
                    # content should be empty
                    assert len(content) == 0


def _setup_data_multiple_population_sources(pulp):
    ubi_repo = create_and_insert_repo(
        id="ubi_repo",
        pulp=pulp,
        population_sources=[
            "rhel_repo-1",
            "rhel_repo-2",
            "rhel_repo-other-1",
            "rhel_repo-other-2",
        ],
        relative_url="foo/bar/os",
        ubi_config_version="8.4",
        content_set="cs_rpm_out",
    )
    distributor_rhel_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_repo-1",
        relative_url="foo/rhel-1/os",
    )
    distributor_rhel_2 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_repo-2",
        relative_url="foo/rhel-2/os",
    )
    rhel_repo_1 = create_and_insert_repo(
        id=distributor_rhel_1.repo_id,
        pulp=pulp,
        content_set="cs_rpm_in",
        relative_url=distributor_rhel_1.relative_url,
        distributors=[distributor_rhel_1],
    )
    rhel_repo_2 = create_and_insert_repo(
        id=distributor_rhel_2.repo_id,
        pulp=pulp,
        content_set="cs_rpm_in",
        relative_url=distributor_rhel_2.relative_url,
        distributors=[distributor_rhel_2],
    )

    distributor_rhel_other_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_repo-other-1",
        relative_url="foo/rhel-other-1/os",
    )
    distributor_rhel_other_2 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_repo-other-2",
        relative_url="foo/rhel-other-2/os",
    )

    rhel_repo_other_1 = create_and_insert_repo(
        id=distributor_rhel_other_1.repo_id,
        pulp=pulp,
        content_set="cs_rpm_in_other",
        relative_url=distributor_rhel_other_1.relative_url,
        distributors=[distributor_rhel_other_1],
    )
    rhel_repo_other_2 = create_and_insert_repo(
        id=distributor_rhel_other_2.repo_id,
        pulp=pulp,
        content_set="cs_rpm_in_other",
        relative_url=distributor_rhel_other_2.relative_url,
        distributors=[distributor_rhel_other_2],
    )

    distributor_debug = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="ubi_debug_repo",
        relative_url="foo/bar/debug",
    )

    ubi_debug_repo = create_and_insert_repo(
        id=distributor_debug.repo_id,
        pulp=pulp,
        population_sources=[
            "rhel_debug_repo-1",
            "rhel_debug_repo-2",
            "rhel_debug_repo-other-1",
            "rhel_debug_repo-other-2",
        ],
        relative_url=distributor_debug.relative_url,
        distributors=[distributor_debug],
        content_set="cs_debug_out",
    )

    distributor_rhel_debug_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_debug_repo-1",
        relative_url="foo/rhel-1/debug",
    )
    distributor_rhel_debug_2 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_debug_repo-2",
        relative_url="foo/rhel-2/debug",
    )
    rhel_debug_repo_1 = create_and_insert_repo(
        id=distributor_rhel_debug_1.repo_id,
        pulp=pulp,
        content_set="cs_debug_in",
        relative_url=distributor_rhel_debug_1.relative_url,
        distributors=[distributor_rhel_debug_1],
    )
    rhel_debug_repo_2 = create_and_insert_repo(
        id=distributor_rhel_debug_2.repo_id,
        pulp=pulp,
        content_set="cs_debug_in",
        relative_url=distributor_rhel_debug_2.relative_url,
        distributors=[distributor_rhel_debug_2],
    )

    distributor_rhel_debug_other_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_debug_repo-other-1",
        relative_url="foo/rhel-other-1/debug",
    )
    distributor_rhel_debug_other_2 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_debug_repo-other-2",
        relative_url="foo/rhel-other-2/debug",
    )
    rhel_debug_repo_other_1 = create_and_insert_repo(
        id=distributor_rhel_debug_other_1.repo_id,
        pulp=pulp,
        content_set="cs_debug_in_other",
        relative_url=distributor_rhel_debug_other_1.relative_url,
        distributors=[distributor_rhel_debug_other_1],
    )
    rhel_debug_repo_other_2 = create_and_insert_repo(
        id=distributor_rhel_debug_other_2.repo_id,
        pulp=pulp,
        content_set="cs_debug_in_other",
        relative_url=distributor_rhel_debug_other_2.relative_url,
        distributors=[distributor_rhel_debug_other_2],
    )

    distributor_source = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="ubi_source_repo",
        relative_url="foo/bar/source/SRPMS",
    )

    ubi_source_repo = create_and_insert_repo(
        id=distributor_source.repo_id,
        pulp=pulp,
        population_sources=[
            "rhel_source_repo",
            "rhel_source_repo-other",
        ],
        relative_url=distributor_source.relative_url,
        ubi_config_version="8.4",
        distributors=[distributor_source],
        content_set="cs_source_out",
    )

    distributor_rhel_source_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_source_repo",
        relative_url="foo/rhel-1/source/SRPMS",
    )
    distributor_rhel_source_2 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_source_repo",
        relative_url="foo/rhel-2/source/SRPMS",
    )
    rhel_source_repo = create_and_insert_repo(
        id=distributor_rhel_source_1.repo_id,
        pulp=pulp,
        content_set="cs_srpm_in",
        distributors=[distributor_rhel_source_1, distributor_rhel_source_2],
    )

    distributor_rhel_source_other_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_source_repo-other",
        relative_url="foo/rhel-other-1/source/SRPMS",
    )
    distributor_rhel_source_other_2 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_source_repo-other",
        relative_url="foo/rhel-other-2/source/SRPMS",
    )
    rhel_source_repo_other = create_and_insert_repo(
        id=distributor_rhel_source_other_1.repo_id,
        pulp=pulp,
        content_set="cs_srpm_in_other",
        distributors=[distributor_rhel_source_other_1, distributor_rhel_source_other_2],
    )

    unit_1 = RpmUnit(
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

    unit_2 = RpmUnit(
        name="gcc",
        version="11",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="gcc-11.200.x86_64.rpm",
        sourcerpm="gcc_src-2-0.src.rpm",
    )

    unit_3 = RpmUnit(
        name="bind",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        sourcerpm="bind_src-1-0.src.rpm",
        filename="bind-10.200.x86_64.rpm",
        requires=[],
        provides=[],
    )

    unit_4 = RpmUnit(
        name="bind",
        version="11",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="bind-11.200.x86_64.rpm",
        sourcerpm="bind_src-2-0.src.rpm",
    )

    unit_debuginfo_1 = RpmUnit(
        name="gcc-debuginfo",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="gcc-debuginfo-10.200.x86_64.rpm",
        sourcerpm="gcc_src-1-0.src.rpm",
    )
    unit_debuginfo_2 = RpmUnit(
        name="gcc-debuginfo",
        version="11",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="gcc-debuginfo-11.200.x86_64.rpm",
        sourcerpm="gcc_src-1-0.src.rpm",
    )
    unit_debugsource_1 = RpmUnit(
        name="gcc_src-debugsource",
        version="11",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="gcc_src-debugsource-11.200.x86_64.rpm",
        sourcerpm="gcc_src_debug-1-0.src.rpm",
    )
    unit_debugsource_2 = RpmUnit(
        name="gcc_src-debugsource",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="gcc_src-debugsource-10.200.x86_64.rpm",
        sourcerpm="gcc_src_debug-1-0.src.rpm",
    )

    unit_debuginfo_3 = RpmUnit(
        name="bind-debuginfo",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="bind-debuginfo-10.200.x86_64.rpm",
        sourcerpm="bind_src-2-0.src.rpm",
    )
    unit_debuginfo_4 = RpmUnit(
        name="bind-debuginfo",
        version="11",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="bind-debuginfo-11.200.x86_64.rpm",
        sourcerpm="bind_src-1-0.src.rpm",
    )
    unit_debugsource_3 = RpmUnit(
        name="bind_src-debugsource",
        version="11",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="bind_src-debugsource-11.200.x86_64.rpm",
        sourcerpm="bind_src_debug-2-0.src.rpm",
    )
    unit_debugsource_4 = RpmUnit(
        name="bind_src-debugsource",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="bind_src-debugsource-10.200.x86_64.rpm",
        sourcerpm="bind_src_debug-1-0.src.rpm",
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
    unit_srpm_debug = RpmUnit(
        name="gcc_src_debug",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="gcc_src_debug-1-0.src.rpm",
        content_type_id="srpm",
    )

    unit_srpm_other = RpmUnit(
        name="bind_src",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="bind_src-2-0.src.rpm",
        content_type_id="srpm",
    )

    unit_srpm_debug_other = RpmUnit(
        name="bind_src_debug",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        requires=[],
        provides=[],
        filename="bind_src_debug-2-0.src.rpm",
        content_type_id="srpm",
    )

    pulp.insert_units(rhel_repo_1, [unit_1])
    pulp.insert_units(rhel_repo_2, [unit_2])
    pulp.insert_units(rhel_repo_other_1, [unit_3])
    pulp.insert_units(rhel_repo_other_2, [unit_4])

    pulp.insert_units(rhel_debug_repo_1, [unit_debuginfo_1, unit_debugsource_1])
    pulp.insert_units(rhel_debug_repo_2, [unit_debuginfo_2, unit_debugsource_2])
    pulp.insert_units(rhel_debug_repo_other_1, [unit_debuginfo_3, unit_debugsource_3])
    pulp.insert_units(rhel_debug_repo_other_2, [unit_debuginfo_4, unit_debugsource_4])

    pulp.insert_units(rhel_source_repo, [unit_srpm, unit_srpm_debug])
    pulp.insert_units(rhel_source_repo_other, [unit_srpm_other, unit_srpm_debug_other])


def test_multiple_population_sources(pulp):
    """Test more complicated scenario when the output repo is expected
    to be populated with multiple input repos. Each input repo can have different content set
    and content config. Configs cannot be mixed between repos with different input content set.
    """
    _setup_data_multiple_population_sources(pulp)

    with mock.patch("ubi_manifest.worker.utils.Client") as client:
        with mock.patch("ubiconfig.get_loader", return_value=MockLoader()):
            with mock.patch(
                "ubi_manifest.worker.tasks.depsolve.redis.from_url"
            ) as mock_redis_from_url:
                redis = MockedRedis(data={})
                mock_redis_from_url.return_value = redis

                client.return_value = pulp.client
                # let run the depsolve task
                result = depsolve.depsolve_task(["ubi_repo"], "fake-url")
                # we don't return anything useful, everything is saved in redis
                assert result is None

                # there should 3 keys stored in redis
                assert sorted(redis.keys()) == [
                    "ubi_debug_repo",
                    "ubi_repo",
                    "ubi_source_repo",
                ]

                # load json string stored in redis
                data = redis.get("ubi_repo")
                content = sorted(json.loads(data), key=lambda d: d["value"])
                # binary repo contains only 2 rpms but each unit has different src_repo_id
                assert len(content) == 2

                unit = content[0]
                assert unit["src_repo_id"] == "rhel_repo-other-2"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "bind-11.200.x86_64.rpm"

                unit = content[1]
                assert unit["src_repo_id"] == "rhel_repo-2"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "gcc-11.200.x86_64.rpm"

                # load json string stored in redis
                data = redis.get("ubi_debug_repo")
                content = sorted(json.loads(data), key=lambda d: d["value"])

                # debuginfo repo contains 4 debug packages
                # `bind`` pkgs from rhel_debug_repo-other* repos different content set and config)
                # `gcc` pkgs from rhel_debug_repo* repo (different content set and config)
                assert len(content) == 4

                unit = content[0]
                assert unit["src_repo_id"] == "rhel_debug_repo-other-2"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "bind-debuginfo-11.200.x86_64.rpm"

                unit = content[1]
                assert unit["src_repo_id"] == "rhel_debug_repo-other-1"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "bind_src-debugsource-11.200.x86_64.rpm"

                unit = content[2]
                assert unit["src_repo_id"] == "rhel_debug_repo-2"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "gcc-debuginfo-11.200.x86_64.rpm"

                unit = content[3]
                assert unit["src_repo_id"] == "rhel_debug_repo-1"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "gcc_src-debugsource-11.200.x86_64.rpm"

                # load json string stored in redis
                data = redis.get("ubi_source_repo")
                content = sorted(json.loads(data), key=lambda d: d["value"])
                # source repo contain 4 SRPM packages, no duplicates, correct src_repo_ids
                assert len(content) == 4
                unit = content[0]
                assert unit["src_repo_id"] == "rhel_source_repo-other"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "bind_src-2-0.src.rpm"

                unit = content[1]
                assert unit["src_repo_id"] == "rhel_source_repo-other"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "bind_src_debug-2-0.src.rpm"

                unit = content[2]
                assert unit["src_repo_id"] == "rhel_source_repo"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "gcc_src-1-0.src.rpm"

                unit = content[3]
                assert unit["src_repo_id"] == "rhel_source_repo"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "gcc_src_debug-1-0.src.rpm"


def test_missing_content_config(pulp):
    """Exception is raised where there is no matching ubi content config"""
    _setup_repos_missing_config(pulp)

    with mock.patch("ubi_manifest.worker.utils.Client") as client:
        with mock.patch("ubiconfig.get_loader", return_value=MockLoader()):
            with mock.patch(
                "ubi_manifest.worker.tasks.depsolve.redis.from_url"
            ) as mock_redis_from_url:
                redis = MockedRedis(data={})
                mock_redis_from_url.return_value = redis

                client.return_value = pulp.client
                # let run the depsolve task
                with pytest.raises(ContentConfigMissing):
                    _ = depsolve.depsolve_task(["ubi_repo"], "fake-url")


def _setup_repos_missing_config(pulp):
    ubi_repo = create_and_insert_repo(
        id="ubi_repo",
        pulp=pulp,
        population_sources=["rhel_repo"],
        relative_url="foo/bar/os",
        ubi_config_version="8.4",
        content_set="cs_rpm_out_missing",
    )
    distributor_rhel = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_repo",
        relative_url="foo/rhel/os",
    )
    rhel_repo = create_and_insert_repo(
        id="rhel_repo",
        pulp=pulp,
        content_set="cs_rpm_in",
        relative_url="foo/rhel/os",
        distributors=[distributor_rhel],
    )

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
        content_set="cs_debug_out",
    )

    distributor_rhel_debug = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_debug_repo",
        relative_url="foo/rhel/debug",
    )
    rhel_debug_repo = create_and_insert_repo(
        id="rhel_debug_repo",
        pulp=pulp,
        content_set="cs_debug_in",
        relative_url="foo/rhel/debug",
        distributors=[distributor_rhel_debug],
    )

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
        content_set="cs_srpm_out",
    )
    rhel_source_repo = create_and_insert_repo(
        id="rhel_source_repo", pulp=pulp, content_set="cs_srpm_in"
    )


def test_multiple_population_sources_skip_depsolving(pulp):
    _setup_data_multiple_population_sources(pulp)

    with mock.patch("ubi_manifest.worker.utils.Client") as client:
        with mock.patch(
            "ubiconfig.get_loader",
            return_value=MockLoader(flags={"base_pkgs_only": True}),
        ):
            with mock.patch(
                "ubi_manifest.worker.tasks.depsolve.redis.from_url"
            ) as mock_redis_from_url:
                redis = MockedRedis(data={})
                mock_redis_from_url.return_value = redis

                client.return_value = pulp.client
                # let run the depsolve task
                result = depsolve.depsolve_task(["ubi_repo"], "fake-url")
                # we don't return anything useful, everything is saved in redis
                assert result is None

                # there should 3 keys stored in redis
                assert sorted(redis.keys()) == [
                    "ubi_debug_repo",
                    "ubi_repo",
                    "ubi_source_repo",
                ]

                # load json string stored in redis
                data = redis.get("ubi_repo")
                content = sorted(json.loads(data), key=lambda d: d["value"])
                # binary repo contains only 2 rpms but each unit has different src_repo_id
                assert len(content) == 2

                unit = content[0]
                assert unit["src_repo_id"] == "rhel_repo-other-2"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "bind-11.200.x86_64.rpm"

                unit = content[1]
                assert unit["src_repo_id"] == "rhel_repo-2"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "gcc-11.200.x86_64.rpm"

                # load json string stored in redis
                data = redis.get("ubi_debug_repo")
                content = sorted(json.loads(data), key=lambda d: d["value"])

                # debuginfo repo is empty because by using flag "base_pkgs_only": True we don't allow
                # adding additional debug pkgs by guessing their names, but only we allow pkgs defined
                # in config
                assert len(content) == 0

                # load json string stored in redis
                data = redis.get("ubi_source_repo")
                content = sorted(json.loads(data), key=lambda d: d["value"])
                # source repo contain 1 SRPM package, correct src_repo_ids
                # SRPM for gcc packge is not available
                assert len(content) == 1
                unit = content[0]
                assert unit["src_repo_id"] == "rhel_source_repo-other"
                assert unit["unit_type"] == "RpmUnit"
                assert unit["unit_attr"] == "filename"
                assert unit["value"] == "bind_src-2-0.src.rpm"


@pytest.mark.parametrize(
    "flags, consistent",
    [
        ({("repo_1", "cs"): {"flag": True}, ("repo_2", "cs"): {"flag": False}}, False),
        (
            {("repo_1", "cs"): {"flag": True}, ("repo_2", "cs"): {"other_flag": "foo"}},
            False,
        ),
        ({("repo_1", "cs"): {"flag": True}, ("repo_2", "cs"): {}}, False),
        ({("repo_1", "cs"): {"flag": True}, ("repo_2", "cs"): {"flag": True}}, True),
    ],
)
def test_validate_depsolver_flags(flags, consistent):
    _test_call = partial(depsolve.validate_depsolver_flags, flags)
    if consistent:
        _test_call()  # no exception raised
    else:
        with pytest.raises(depsolve.InconsistentDepsolverConfig):
            _test_call()
