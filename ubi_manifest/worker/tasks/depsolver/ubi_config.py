from typing import List

import ubiconfig


class UbiConfigLoader:
    """
    Class capable of loading UbiConfig from git repository at given url.
    """

    def __init__(self, url: str) -> None:
        self._url: str = url
        self._config_map: dict = {}
        self._all_config: List[ubiconfig.UbiConfig] = None

    @property
    def all_config(self) -> List[ubiconfig.UbiConfig]:
        if self._all_config is None:
            self._all_config = self._load_all()

        return self._all_config

    def _load_all(self) -> List[ubiconfig.UbiConfig]:
        loader = ubiconfig.get_loader(self._url)
        return loader.load_all()

    def get_config(self, content_set_name: str, version: str) -> ubiconfig.UbiConfig:
        """Gets and returns UbiConfig for given content_set_name and version"""
        out = self._config_map.get((content_set_name, version)) or None

        if out is None:
            for config in self.all_config:
                for cs_name in self._all_content_sets_in_config(config):
                    self._config_map.setdefault((cs_name, config.version), config)

                if config.version == version:
                    if content_set_name in self._all_content_sets_in_config(config):
                        out = config
                        break

        return out

    @staticmethod
    def _all_content_sets_in_config(config: ubiconfig.UbiConfig) -> List[str]:
        return [
            config.content_sets.rpm.input,
            config.content_sets.debuginfo.input,
            config.content_sets.srpm.input,
            config.content_sets.rpm.output,
            config.content_sets.debuginfo.output,
            config.content_sets.srpm.output,
        ]
