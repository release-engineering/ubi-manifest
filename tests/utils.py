from unittest.mock import Mock

import ubiconfig
from attrs import define
from pubtools.pulplib import RpmDependency, YumRepository


def create_and_insert_repo(**kwargs):
    pulp = kwargs.pop("pulp")
    pulp.insert_repository(YumRepository(**kwargs))

    return pulp.client.get_repository(kwargs["id"])


def create_mock_configs(n, flags=None, versions=None):
    """
    Creates n mock config objects with the given flags.
    """
    configs = []
    if not flags:
        flags = [{} for _i in range(n)]
    if not versions:
        versions = ["8" for _i in range(n)]

    for i in range(n):
        config = Mock(
            version=versions[i],
            flags=Mock(as_dict=Mock(return_value=flags[i])),
            content_sets=Mock(rpm=Mock(output=f"content_set_{i}")),
        )
        configs.append(config)

    return configs


class MockLoader:
    def __init__(self, flags=None):
        self.flags = {"flags": flags or {}}

    def load_all(self):
        config_raw_1 = {
            "modules": {
                "include": [
                    {
                        "name": "fake_module",
                        "stream": "1",
                    },
                    {  # content audit should permit multiple same modules with different streams
                        "name": "fake_module",
                        "stream": "3",
                    },
                    {  # This should not be reported as missing in content audit tests.
                        "name": "bind",
                        "stream": "12",
                    },
                ]
            },
            "packages": {
                "include": [
                    "package-name-.*",
                    "gcc.*",
                    "httpd.src",
                    "pkg-debuginfo.*",
                    "bash.*",
                    "neovim.*",
                ],
                "exclude": [
                    "package-name*.*",
                    "kernel",
                    "kernel.x86_64",
                    "kernel.src",
                    "gcc.src",
                ],
            },
            "content_sets": {
                "rpm": {"output": "cs_rpm_out", "input": "cs_rpm_in"},
                "srpm": {"output": "cs_srpm_out", "input": "cs_srpm_in"},
                "debuginfo": {"output": "cs_debug_out", "input": "cs_debug_in"},
            },
            "arches": ["x86_64", "src"],
        }
        config_raw_1.update(self.flags)

        config_raw_2 = {
            "modules": {
                "include": [
                    {
                        "name": "fake_name",
                        "stream": "fake_stream",
                    }
                ]
            },
            "packages": {
                "include": [
                    "package-name-.*",
                    "gcc.*",
                    "httpd.src",
                    "pkg-debuginfo.*",
                    "bind.*",
                    "neovim.*",
                    "bash.*",
                ],
                "exclude": [
                    "package-name*.*",
                    "kernel",
                    "kernel.x86_64",
                    "kernel.src",
                    "gcc.src",
                ],
            },
            "content_sets": {
                "rpm": {"output": "cs_rpm_out", "input": "cs_rpm_in_other"},
                "srpm": {"output": "cs_srpm_out", "input": "cs_srpm_in_other"},
                "debuginfo": {"output": "cs_debug_out", "input": "cs_debug_in_other"},
            },
            "arches": ["x86_64", "src"],
        }
        config_raw_2.update(self.flags)

        return [
            ubiconfig.UbiConfig.load_from_dict(config, file, "8")
            for config, file in [(config_raw_1, "file_1"), (config_raw_2, "file_2")]
        ]


@define
class MockedRedis:
    data: dict
    ping_fail: bool = False

    def set(self, key: str, value: str, **kwargs) -> None:
        self.data[key] = value

    def get(self, key: str) -> str:
        return self.data.get(key)

    def keys(self) -> list[str]:
        return list(self.data.keys())

    def ping(self) -> bool:
        if self.ping_fail:
            raise ConnectionError("Connection refused.")
        return True

    def exists(self, key: str) -> bool:
        keys = self.keys()
        if key not in keys:
            return False
        return True


def rpmdeps_from_names(*names):
    return {RpmDependency(name=name) for name in names}
