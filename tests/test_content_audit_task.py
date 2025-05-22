import logging
from unittest import mock

import pytest
from pubtools.pulplib import Distributor, ModulemdUnit, RpmUnit

from tests.utils import MockLoader, create_and_insert_repo
from ubi_manifest.worker.tasks.content_audit import content_audit_task


def test_pipeline(pulp, caplog):
    caplog.set_level(logging.DEBUG, logger="ubi_manifest.worker.tasks.auditing")

    # Input repos
    distributor_rhel_binary_repo_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_bin_repo-1",
        relative_url="/location/rhel_repo-1/os",
    )
    distributor_rhel_debug_repo_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="rhel_debug_repo-1",
        relative_url="/location/rhel_repo-1/debug",
    )
    rhel_binary_repo_1 = create_and_insert_repo(
        id=distributor_rhel_binary_repo_1.repo_id,
        pulp=pulp,
        content_set="cs_rpm_in",
        relative_url=distributor_rhel_binary_repo_1.relative_url,
        distributors=[distributor_rhel_binary_repo_1],
        arch="x86_64",
    )
    rhel_debug_repo_1 = create_and_insert_repo(
        id=distributor_rhel_debug_repo_1.repo_id,
        pulp=pulp,
        content_set="cs_debug_in",
        relative_url=distributor_rhel_debug_repo_1.relative_url,
        distributors=[distributor_rhel_debug_repo_1],
        arch="x86_64",
    )

    # Output repos
    distributor_ubi_binary_repo_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="ubi_bin_repo-1",
        relative_url="/location/ubi_repo-1/os",
    )
    distributor_ubi_source_repo_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="ubi_source_repo-1",
        relative_url="/location/ubi_repo-1/source/SRPMS",
    )
    distributor_ubi_debug_repo_1 = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="ubi_debug_repo-1",
        relative_url="/location/ubi_repo-1/debug",
    )
    # modular
    ubi_binary_repo_1 = create_and_insert_repo(
        pulp=pulp,
        id=distributor_ubi_binary_repo_1.repo_id,
        population_sources=["rhel_bin_repo-1"],
        distributors=[distributor_ubi_binary_repo_1],
        ubi_population=True,
        relative_url=distributor_ubi_binary_repo_1.relative_url,
        ubi_config_version="8",
        content_set="cs_rpm_out",
        arch="x86_64",
    )
    # non modular
    ubi_source_repo_1 = create_and_insert_repo(
        pulp=pulp,
        id=distributor_ubi_source_repo_1.repo_id,
        population_sources=["rhel_source_repo-1"],
        distributors=[distributor_ubi_source_repo_1],
        ubi_population=True,
        relative_url=distributor_ubi_source_repo_1.relative_url,
        ubi_config_version="8",
        content_set="cs_srpm_out",
        arch="src",
    )
    # non modular
    ubi_debug_repo_1 = create_and_insert_repo(
        pulp=pulp,
        id=distributor_ubi_debug_repo_1.repo_id,
        population_sources=["rhel_debug_repo-1"],
        distributors=[distributor_ubi_debug_repo_1],
        ubi_population=True,
        relative_url=distributor_ubi_debug_repo_1.relative_url,
        ubi_config_version="8",
        content_set="cs_debug_out",
        arch="x86_64",
    )

    module_1 = ModulemdUnit(
        name="fake_module-1",
        stream="1",
        version=10,
        context="b7fad3bf",
        arch="x86_64",
        artifacts=[
            "bash-0:5.0.7-1.module+el8+2240+23e6f3c3.src",
            "bash-0:5.0.7-1.module+el8+2240+23e6f3c3.noarch",
            "bash-debuginfo-0:5.0.7-1.module+el8+2240+23e6f3c3.noarch",
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

    output_only_rpm = RpmUnit(
        name="neovim",
        version="0.10.4",
        release="1.fc41",
        arch="x86_64",
    )
    input_only_rpm = RpmUnit(
        name="bash",
        version="5.0.7",
        release="1.fc30",
        arch="x86_64",
        filename="bash-5.0.7.1.fc30.x86_64.rpm",
    )
    bash_debuginfo_rpm = RpmUnit(
        name="bash-debuginfo.foo",
        version="5.0.7",
        release="1.fc30",
        arch="x86_64",
    )
    bash_noarch = RpmUnit(
        name="bash-0",
        version="5.0.7",
        release="1.module+el8+2240+23e6f3c3",
        arch="noarch",
        filename="bash-0:5.0.7-1.module+el8+2240+23e6f3c3.noarch",
    )

    pulp.insert_units(
        rhel_binary_repo_1,
        [gcc_rpm_current, bind_rpm, module_1, input_only_rpm, bash_noarch],
    )
    pulp.insert_units(
        rhel_debug_repo_1,
        [
            pkg_debuginfo_rpm,
            bash_debuginfo_rpm,
            blacklisted_abc_rpm,
        ],
    )

    pulp.insert_units(
        ubi_binary_repo_1,
        [output_only_rpm, gcc_rpm_outdated, bind_rpm, module_1, bash_noarch],
    )
    pulp.insert_units(
        ubi_debug_repo_1,
        [
            pkg_debuginfo_rpm,
            bash_debuginfo_rpm,
            blacklisted_abc_rpm,
        ],
    )

    with mock.patch("ubi_manifest.worker.utils.Client") as client:
        with mock.patch("ubiconfig.get_loader", return_value=MockLoader()):
            client.return_value = pulp.client

            # should run without error
            content_audit_task()

            expected_ubi_binary_repo_1 = [
                f"Processing and auditing UBI repo '{ubi_binary_repo_1.id}' with modular content...",
                "Only auditing of non modular content has been implemented.",
                f"[{ubi_binary_repo_1.id}] UBI rpm of {ubi_binary_repo_1.arch} 'gcc' is outdated (current: ('0', '8.2.1', '200'), latest: ('0', '9.0.1', '200'))",
                f"[{ubi_binary_repo_1.id}] Whitelisted package 'neovim' found in out repo but not in any input repos!",
                f"[{ubi_binary_repo_1.id}] Whitelisted package 'bash' found in input repos but not in output repo!",
            ]
            expected_ubi_source_repo_1 = [
                f"Skipping auditing of source repo '{ubi_source_repo_1.id}': Not implemented yet."
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

            assert (
                f"Auditing bundle of UBI repos [{ubi_binary_repo_1.id}, {ubi_debug_repo_1.id}, {ubi_source_repo_1.id}]..."
                in caplog.text
            )

            for log in expected_logs:
                assert log in caplog.text


def test_pipeline_with_invalid_repo(pulp):
    distributor_bin = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="unknown_out_repo",
        relative_url="/location/bar/os",
    )
    distributor_source = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="unknown_out_repo-source",
        relative_url="/location/bar/source/SRPMS",
    )
    distributor_debug = Distributor(
        id="yum_distributor",
        type_id="yum_distributor",
        repo_id="unknown_out_repo-debug",
        relative_url="/location/bar/debug",
    )

    create_and_insert_repo(
        pulp=pulp,
        id=distributor_bin.repo_id,
        population_sources=["unknown_in_repo"],
        distributors=[distributor_bin],
        ubi_population=True,
        relative_url=distributor_bin.relative_url,
        ubi_config_version="8",
        content_set="cs_rpm_out",
        arch="x86_64",
    )
    create_and_insert_repo(
        pulp=pulp,
        id=distributor_source.repo_id,
        population_sources=["unknown_in_repo"],
        distributors=[distributor_source],
        ubi_population=True,
        relative_url=distributor_source.relative_url,
        ubi_config_version="8",
        content_set="cs_srpm_out",
        arch="x86_64",
    )
    create_and_insert_repo(
        pulp=pulp,
        id=distributor_debug.repo_id,
        population_sources=["unknown_in_repo"],
        distributors=[distributor_debug],
        ubi_population=True,
        relative_url=distributor_debug.relative_url,
        ubi_config_version="8",
        content_set="cs_debug_out",
        arch="x86_64",
    )

    with mock.patch("ubi_manifest.worker.utils.Client") as client:
        client.return_value = pulp.client

        with pytest.raises(ValueError, match="unexpected id"):
            content_audit_task()
