import ubiconfig
from pubtools.pulplib import YumRepository


def create_and_insert_repo(**kwargs):
    pulp = kwargs.pop("pulp")
    pulp.insert_repository(YumRepository(**kwargs))

    return pulp.client.get_repository(kwargs["id"])


class MockLoader:
    def load_all(self):
        config_raw = {
            "modules": {},
            "packages": {
                "include": ["package-name-.*", "gcc.*", "httpd.src", "pkg-debuginfo.*"],
                "exclude": ["package-name*.*", "kernel", "kernel.x86_64"],
            },
            "content_sets": {
                "rpm": {"output": "rpm_out", "input": "rpm_in"},
                "srpm": {"output": "srpm_out", "input": "srpm_in"},
                "debuginfo": {"output": "debug_out", "input": "debug_in"},
            },
            "arches": ["x86_64", "src"],
        }

        return [ubiconfig.UbiConfig.load_from_dict(config_raw, "foo", "8")]
