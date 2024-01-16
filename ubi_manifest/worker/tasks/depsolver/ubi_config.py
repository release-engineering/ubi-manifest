from typing import Optional, Any

import ubiconfig


class UbiConfigLoader:
    """
    Class capable of loading UbiConfig from git repository at given url.
    """

    def __init__(self, url: str) -> None:
        self._url: str = url
        self._config_map: dict[tuple[str, str, str], ubiconfig.UbiConfig] = {}
        self._all_config: Optional[list[ubiconfig.UbiConfig]] = None

    @property
    def all_config(self) -> list[ubiconfig.UbiConfig]:
        if self._all_config is None:
            self._all_config = self._load_all()

        return self._all_config

    def _load_all(self) -> Any:
        loader = ubiconfig.get_loader(self._url)
        return loader.load_all()

    def get_config(
        self, input_cs: str, output_cs: str, version: str
    ) -> ubiconfig.UbiConfig:
        """Gets and returns UbiConfig for given input content set, output content set and a version"""
        out = self._config_map.get((input_cs, output_cs, version)) or None

        if out is None:
            for config in self.all_config:
                for cs_in, cs_out in self._content_sets(config):
                    self._config_map.setdefault((cs_in, cs_out, config.version), config)

                if config.version == version:
                    if (
                        input_cs,
                        output_cs,
                    ) in self._content_sets(config):
                        out = config
                        break

        return out

    @staticmethod
    def _content_sets(config: ubiconfig.UbiConfig) -> list[tuple[str, str]]:
        return [
            (
                config.content_sets.rpm.input,
                config.content_sets.rpm.output,
            ),
            (
                config.content_sets.debuginfo.input,
                config.content_sets.debuginfo.output,
            ),
            (
                config.content_sets.srpm.input,
                config.content_sets.srpm.output,
            ),
        ]
