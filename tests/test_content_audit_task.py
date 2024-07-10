import logging
from unittest import mock

from pubtools.pulplib import Distributor, ModulemdDefaultsUnit, ModulemdUnit, RpmUnit

from ubi_manifest.worker.tasks.content_audit import content_audit_task

from .utils import MockLoader, create_and_insert_repo


def _setup_population_sources(pulp):
    ubi_repo = create_and_insert_repo(
        pulp=pulp,
        id="ubi_repo",
        population_sources=[
            "rhel_repo-1",
            "rhel_repo-2",
        ],
        ubi_population=True,
        relative_url="foo/bar/os",
        ubi_config_version="8",
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

    rpm_1 = RpmUnit(
        name="gcc",
        version="9.0.1",
        release="200",
        epoch="1",
        arch="x86_64",
        sourcerpm="gcc_src-1-0.src.rpm",
        filename="gcc-10.200.x86_64.rpm",
        requires=[],
        provides=[],
    )
    rpm_2 = RpmUnit(
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
    rpm_3 = RpmUnit(  # modular, should be skipped
        name="bind",
        version="12",
        release="2.module+el8+2248+23d5e2f2",
        epoch="0",
        arch="x86_64",
        sourcerpm="bind-12-2.module+el8+2248+23d5e2f2.src.rpm",
        filename="bind-12-2.module+el8+2248+23d5e2f2.noarch.rpm",
    )
    rpm_4 = RpmUnit(name="httpd.src", version="1", release="2", arch="x86_64")
    rpm_5 = RpmUnit(name="pkg-debuginfo.foo", version="1", release="2", arch="x86_64")
    rpm_6 = RpmUnit(name="package-name-abc", version="1", release="2", arch="x86_64")
    module_1 = ModulemdUnit(
        name="fake_name",
        stream="fake_stream",
        version=10,
        context="b7fad3bf",
        arch="x86_64",
        artifacts=[
            "test-0:1.24-3.module+el8.1.0+2934+dec45db7.noarch",
            "test-0:1.24-3.module+el8.1.0+2934+dec45db7.src",
            "bind-0:12-2.module+el8+2248+23d5e2f2.noarch",
            "bind-0:12-2.module+el8+2248+23d5e2f2.src",
        ],
    )
    module_2 = ModulemdUnit(
        name="some_module1",
        stream="fake_stream",
        version=10,
        context="b7fad3bf",
        arch="x86_64",
        artifacts=[
            "test-1:1.24-3.module+el8.1.0+2934+dec45db7.noarch",
            "test-1:1.24-3.module+el8.1.0+2934+dec45db7.src",
        ],
    )
    default_1 = ModulemdDefaultsUnit(
        name="some_module_defaults1",
        stream="fake_stream",
        repo_id="ubi_repo",
        profiles={"1.1": ["default"], "1.0": []},
    )
    default_2 = ModulemdDefaultsUnit(
        name="some_module_defaults2",
        stream="fake_stream",
        repo_id="ubi_repo",
        profiles={"1.0": ["default"]},
    )

    pulp.insert_units(rhel_repo_1, [rpm_1, rpm_3, rpm_5, module_1, default_1])
    pulp.insert_units(rhel_repo_2, [rpm_2, rpm_4, rpm_6, module_2, default_2])
    pulp.insert_units(
        ubi_repo,
        [
            rpm_1,
            rpm_2,
            rpm_3,
            rpm_4,
            rpm_5,
            rpm_6,
            module_1,
            module_2,
            default_1,
            default_2,
        ],
    )


def test_content_audit_outdated(pulp, caplog):
    """
    Test that a run of the content audit task completes without issue and
    reports when content is outdated.
    """

    caplog.set_level(logging.DEBUG, logger="ubi_manifest.worker.tasks.content_audit")
    _setup_population_sources(pulp)

    # populate our outdated UBI repo
    ubi_repo = create_and_insert_repo(
        pulp=pulp,
        id="outdated_ubi_repo",
        population_sources=[
            "rhel_repo-1",
            "rhel_repo-2",
        ],
        ubi_population=True,
        relative_url="foo/bar/os",
        ubi_config_version="8",
        content_set="cs_rpm_out",
    )
    rpm_1 = RpmUnit(
        name="gcc",
        version="8.2.1",  # outdated
        release="200",
        epoch="1",
        arch="x86_64",
        sourcerpm="gcc_src-1-0.src.rpm",
        filename="gcc-10.200.x86_64.rpm",
    )
    rpm_2 = RpmUnit(
        name="bind",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        sourcerpm="bind_src-1-0.src.rpm",
        filename="bind-10.200.x86_64.rpm",
    )
    module_1 = ModulemdUnit(
        name="fake_name",
        stream="fake_stream",
        version=10,
        context="b7fad3bf",
        arch="x86_64",
        artifacts=[
            "test-0:1.24-3.module+el8.1.0+2934+dec45db7.noarch",
            "test-0:1.24-3.module+el8.1.0+2934+dec45db7.src",
            "bind-0:12-2.module+el8+2248+23d5e2f2.noarch",
            "bind-0:12-2.module+el8+2248+23d5e2f2.src",
        ],
    )
    module_2 = ModulemdUnit(
        name="some_module1",
        stream="fake_stream",
        version=7,  # outdated
        context="b7fad3bf",
        arch="x86_64",
        artifacts=[
            "test-0:5.module+el8.1.0+2934+dec45db7.noarch",
            "test-0:5.module+el8.1.0+2934+dec45db7.src",
        ],
    )
    default_1 = ModulemdDefaultsUnit(
        name="some_module_defaults1",
        stream="fake_stream",
        repo_id="outdated_ubi_repo",
        profiles={"1.0": ["default"]},  # outdated
    )
    default_2 = ModulemdDefaultsUnit(
        name="some_module_defaults2",
        stream="fake_stream",
        repo_id="outdated_ubi_repo",
        profiles={"1.0": ["default"]},
    )
    pulp.insert_units(
        ubi_repo,
        [rpm_1, rpm_2, module_1, module_2, default_1, default_2],
    )

    with mock.patch("ubi_manifest.worker.utils.Client") as client:
        with mock.patch("ubiconfig.get_loader", return_value=MockLoader()):
            client.return_value = pulp.client

            # should run without error
            content_audit_task()

        # should have logged warnings
        expected_logs = [
            "[outdated_ubi_repo] Skipping modular RPM bind-12-2.module+el8+2248+23d5e2f2.noarch.rpm",
            "[outdated_ubi_repo] UBI modulemd 'some_module1:fake_stream' version is outdated (current: 7, latest: 10)",
            "[outdated_ubi_repo] UBI modulemd_defaults 'some_module_defaults1:fake_stream' version is outdated",
            "[outdated_ubi_repo] UBI rpm 'gcc' version is outdated (current: ('0', '8.2.1', '200'), latest: ('0', '9.0.1', '200'))",
            # we didn't add RPM 'pkg-debuginfo'
            "[outdated_ubi_repo] whitelisted content missing from UBI and/or population sources;\n\tpkg-debuginfo",
        ]
        for real_msg, expected_msg in zip(sorted(caplog.messages), expected_logs):
            assert expected_msg in real_msg


def test_content_audit_blacklisted(pulp, caplog):
    """
    Test that a run of the content audit task completes without issue and
    reports when content is blacklisted.
    """

    caplog.set_level(logging.DEBUG, logger="ubi_manifest.worker.tasks.content_audit")
    _setup_population_sources(pulp)

    ubi_repo = create_and_insert_repo(
        pulp=pulp,
        id="contaminated_ubi_repo",
        population_sources=[
            "rhel_repo-1",
            "rhel_repo-2",
            "bad_repo",
        ],
        ubi_population=True,
        relative_url="foo/bar/os",
        ubi_config_version="8",
        content_set="cs_rpm_out",
    )
    bad_dist = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="bad_repo",
        relative_url="foo/rhel-2/os",
    )
    bad_repo = create_and_insert_repo(
        id=bad_dist.repo_id,
        pulp=pulp,
        content_set="cs_rpm_in",
        relative_url=bad_dist.relative_url,
        distributors=[bad_dist],
    )
    blacklisted = RpmUnit(
        name="kernel",
        version="10",
        release="200",
        epoch="1",
        arch="x86_64",
        sourcerpm="kernel-1-0.src.rpm",
        filename="kernel-10.200.x86_64.rpm",
        requires=[],
        provides=[],
    )
    pulp.insert_units(bad_repo, [blacklisted])
    pulp.insert_units(ubi_repo, [blacklisted])

    with mock.patch("ubi_manifest.worker.utils.Client") as client:
        with mock.patch("ubiconfig.get_loader", return_value=MockLoader()):
            client.return_value = pulp.client

            # should run without error
            content_audit_task()

        # should have logged a warning
        assert (
            "[contaminated_ubi_repo] blacklisted content found in input repositories;\n\tkernel"
            in caplog.text
        )
