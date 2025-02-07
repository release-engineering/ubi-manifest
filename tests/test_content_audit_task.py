import logging
from unittest import mock

import pytest
from pubtools.pulplib import Distributor, ModulemdUnit, RpmUnit

from tests.utils import MockLoader, create_and_insert_repo
from ubi_manifest.worker.common import get_pkgs_from_all_modules
from ubi_manifest.worker.tasks.content_audit import content_audit_task


def test_pipeline(pulp, caplog):
    caplog.set_level(logging.DEBUG, logger="ubi_manifest.worker.tasks.auditing")

    # Input repos
    distributor_rhel_binary_repo_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_bin_repo-1",
        relative_url="foo/rhel_bin_repo-1",
    )
    distributor_rhel_source_repo_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_source_repo-1",
        relative_url="foo/rhel_source_repo-1",
    )
    distributor_rhel_debug_repo_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_debug_repo-1",
        relative_url="foo/rhel_debug_repo-1",
    )
    rhel_binary_repo_1 = create_and_insert_repo(
        id=distributor_rhel_binary_repo_1.repo_id,
        pulp=pulp,
        content_set="cs_rpm_in",
        relative_url=distributor_rhel_binary_repo_1.relative_url,
        distributors=[distributor_rhel_binary_repo_1],
    )
    # non modular
    rhel_source_repo_1 = create_and_insert_repo(
        id=distributor_rhel_source_repo_1.repo_id,
        pulp=pulp,
        content_set="cs_srpm_in",
        relative_url=distributor_rhel_source_repo_1.relative_url,
        distributors=[distributor_rhel_source_repo_1],
    )
    # non modular
    rhel_debug_repo_1 = create_and_insert_repo(
        id=distributor_rhel_debug_repo_1.repo_id,
        pulp=pulp,
        content_set="cs_debug_in",
        relative_url=distributor_rhel_debug_repo_1.relative_url,
        distributors=[distributor_rhel_debug_repo_1],
    )

    # Output repos
    # modular
    ubi_binary_repo_1 = create_and_insert_repo(
        pulp=pulp,
        id="ubi_bin_repo-1",
        population_sources=["rhel_bin_repo-1"],
        ubi_population=True,
        relative_url="foo/bar/os",
        ubi_config_version="8",
        content_set="cs_rpm_out",
    )
    # non modular
    ubi_source_repo_1 = create_and_insert_repo(
        pulp=pulp,
        id="ubi_source_repo-1",
        population_sources=["rhel_source_repo-1"],
        ubi_population=True,
        relative_url="foo/bar/os",
        ubi_config_version="8",
        content_set="cs_srpm_out",
    )
    # non modular
    ubi_debug_repo_1 = create_and_insert_repo(
        pulp=pulp,
        id="ubi_debug_repo-1",
        population_sources=["rhel_debug_repo-1"],
        ubi_population=True,
        relative_url="foo/bar/os",
        ubi_config_version="8",
        content_set="cs_debug_out",
    )

    # bash-5.0.7-1.fc30.x86_64.rpm
    module_1 = ModulemdUnit(
        name="fake_module-1",
        stream="1",
        version=10,
        context="b7fad3bf",
        arch="x86_64",
        artifacts=[
            "test-0:1.24-3.module+el8.1.0+2934+dec45db7.noarch",
            "test-0:1.24-3.module+el8.1.0+2934+dec45db7.src",
            "bind-0:12-2.module+el8+2248+23d5e2f2.noarch",
            "bind-0:12-2.module+el8+2248+23d5e2f2.src",
            "bash-0:5.0.7-1.module+el8+2240+23e6f3c3.src",
            "bash-0:5.0.7-1.module+el8+2240+23e6f3c3.noarch",
        ],
    )
    module_2 = ModulemdUnit(
        name="some_module1",
        stream="1",
        version=7,  # outdated
        context="b7fad3bf",
        arch="x86_64",
        artifacts=[
            "test-0:5.module+el8.1.0+2934+dec45db7.noarch",
            "test-0:5.module+el8.1.0+2934+dec45db7.src",
        ],
    )
    module_3 = ModulemdUnit(
        name="fake_module",
        stream="3",
        version=10,
        context="b7fad3bf",
        arch="x86_64",
        artifacts=[
            "test-0:4.6-2.module+el8.1.0+2934+dec45db7.noarch",
            "test-0:4.6-2.module+el8.1.0+2934+dec45db7.src",
        ],
    )

    gcc_rpm_current = RpmUnit(
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
    gcc_rpm_outdated = RpmUnit(
        name="gcc",
        version="8.2.1",  # outdated
        release="200",
        epoch="1",
        arch="x86_64",
        sourcerpm="gcc_src-1-0.src.rpm",
        filename="gcc-10.200.x86_64.rpm",
    )
    bind_rpm = RpmUnit(
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
    httpd_srpm_outdated = RpmUnit(
        name="httpd.src",
        version="1",
        release="2",
        arch="x86_64",
    )
    httpd_srpm_current = RpmUnit(
        name="httpd.src",
        version="2",  # newer version
        release="2",
        arch="x86_64",
    )
    pkg_debuginfo_rpm = RpmUnit(
        name="pkg-debuginfo.foo",
        version="1",
        release="2",
        arch="x86_64",
    )
    blacklisted_abc_rpm = RpmUnit(
        name="package-name-abc",
        version="1",
        release="2",
        arch="x86_64",
    )
    # neovim.x86_64 0.10.4-1.fc41
    output_only_rpm = RpmUnit(
        name="neovim",
        version="0.10.4",
        release="1.fc41",
        arch="x86_64",
    )
    # bash-5.0.7-1.fc30.x86_64.rpm
    input_only_rpm = RpmUnit(
        name="bash",
        version="5.0.7",
        release="1.fc30",
        arch="x86_64",
    )

    pulp.insert_units(
        rhel_binary_repo_1, [gcc_rpm_current, bind_rpm, module_1, input_only_rpm]
    )
    pulp.insert_units(
        rhel_source_repo_1,
        [httpd_srpm_current],
    )
    pulp.insert_units(
        rhel_debug_repo_1,
        [
            pkg_debuginfo_rpm,
            blacklisted_abc_rpm,
            module_1,
            module_2,
            module_3,
        ],
    )

    pulp.insert_units(
        ubi_binary_repo_1,
        [output_only_rpm, gcc_rpm_outdated, bind_rpm, module_1],
    )
    pulp.insert_units(
        ubi_source_repo_1,
        [httpd_srpm_outdated],
    )
    pulp.insert_units(
        ubi_debug_repo_1,
        [
            pkg_debuginfo_rpm,
            blacklisted_abc_rpm,
        ],
    )

    with mock.patch("ubi_manifest.worker.utils.Client") as client:
        with mock.patch("ubiconfig.get_loader", return_value=MockLoader()):
            client.return_value = pulp.client

            all_modular_filenames = get_pkgs_from_all_modules(
                [ubi_source_repo_1, ubi_binary_repo_1, ubi_debug_repo_1]
            )  # only binary will contribute

            # should run without error
            content_audit_task()

            expected_ubi_binary_repo_1 = [
                f"Processing and auditing UBI repo '{ubi_binary_repo_1.id}' with modular content...",
                f"Modular filenames: {all_modular_filenames}",
                f"[{ubi_binary_repo_1.id}] UBI rpm 'gcc' is outdated (current: (0, 8.2.1, 200), latest: (0, 9.0.1, 200))",
                f"[{ubi_binary_repo_1.id}] Whitelisted package 'neovim' found in out repo but not in any input repos!",
                f"[{ubi_binary_repo_1.id}] Whitelisted package 'bash' found in input repos but not in output repo!",
            ]

            unexpected_ubi_binary_repo_1 = [
                f"[{ubi_binary_repo_1.id}] Whitelisted package 'bash' not found in any input or output repositories.",  # should not be here
            ]

            expected_ubi_source_repo_1 = [
                f"Processing and auditing UBI repo '{ubi_source_repo_1.id}'...",
                f"[{ubi_source_repo_1.id}] UBI rpm 'httpd.src' is outdated (current: (0, 1, 2), latest: (0, 2, 2))",
                f"[{ubi_source_repo_1.id}] Whitelisted package 'gcc' not found in any input or output repositories.",
                f"[{ubi_source_repo_1.id}] Whitelisted package 'bash' not found in any input or output repositories.",
                f"[{ubi_source_repo_1.id}] Whitelisted package 'neovim' not found in any input or output repositories.",
            ]

            expected_ubi_debug_repo_1 = [
                f"Processing and auditing UBI repo '{ubi_debug_repo_1.id}'...",
                f"[{ubi_debug_repo_1.id}] blacklisted content found in output repository;\n\tpackage-name-abc",
                f"[{ubi_debug_repo_1.id}] Whitelisted package 'pkg-debuginfo' not found in any input or output repositories.",
            ]

            expected_logs = (
                expected_ubi_binary_repo_1
                + expected_ubi_source_repo_1
                + expected_ubi_debug_repo_1
            )

            unexpected_logs = unexpected_ubi_binary_repo_1

            for log in expected_logs:
                assert log in caplog.text
            for bad_log in unexpected_logs:
                assert bad_log not in caplog.text


def test_pipeline_with_invalid_repo(pulp):
    distributor = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="unknown_repo",
        relative_url="foo/unknown",
    )

    unknown_repo = create_and_insert_repo(
        pulp=pulp,
        id=distributor.repo_id,
        population_sources=[],
        ubi_population=True,
        relative_url=distributor.relative_url,
        ubi_config_version="8",
        content_set="cs_unknown",
    )
    ubi_repo = create_and_insert_repo(
        pulp=pulp,
        id="unknown_out_repo",
        population_sources=[distributor.repo_id],
        ubi_population=True,
        relative_url="foo/bar/os",
        ubi_config_version="8",
        content_set="cs_rpm_out",
    )
    pulp.insert_units(unknown_repo, [])
    pulp.insert_units(ubi_repo, [])

    with mock.patch("ubi_manifest.worker.utils.Client") as client:
        with mock.patch("ubiconfig.get_loader", return_value=None):
            client.return_value = pulp.client

            with pytest.raises(ValueError, match="unexpected id"):
                content_audit_task()
