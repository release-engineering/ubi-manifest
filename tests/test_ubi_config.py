from unittest import mock

from ubi_manifest.worker.tasks.depsolver.ubi_config import UbiConfigLoader

from .utils import MockLoader


def test_get_config():
    with mock.patch("ubiconfig.get_loader", return_value=MockLoader()) as mock_loader:
        loader = UbiConfigLoader("https://foo.bar.com/some-repo.git")
        config = loader.get_config("cs_rpm_in", "cs_rpm_out", "8")

        # let's check what we got
        assert config.version == "8"
        assert config.content_sets.rpm.input == "cs_rpm_in"
        assert config.content_sets.rpm.output == "cs_rpm_out"
        assert config.content_sets.srpm.input == "cs_srpm_in"
        assert config.content_sets.srpm.output == "cs_srpm_out"
        assert config.content_sets.debuginfo.input == "cs_debug_in"
        assert config.content_sets.debuginfo.output == "cs_debug_out"

        # there should be three entries in the loader._config_map dict
        # each unique combo of (cs_in, cs_out, version)
        assert len(loader._config_map.keys()) == 3

        # check one of the entry
        config_to_check = loader._config_map[("cs_debug_in", "cs_debug_out", "8")]
        # it should be the same object as the original config
        assert config is config_to_check

        # call it once more again
        _ = loader.get_config("cs_debug_in", "cs_debug_out", "8")

        # mock_loader ("ubiconfig.get_loader") should be called only once
        # second call of loader.get_config() reads from loader._config_map
        mock_loader.assert_called_once()
