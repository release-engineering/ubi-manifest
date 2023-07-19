import configparser
import json
import os
import re
from typing import List, Union

from attrs import define


@define
class Config:
    pulp_url: str = "some_url"
    pulp_username: str = "username"
    pulp_password: str = "pass"
    pulp_cert: str = "path/to/cert"
    pulp_key: str = "path/to/key"
    pulp_verify: Union[bool, str] = True
    content_config: dict = {"group_prefix": "url_to_config_repository"}
    allowed_ubi_repo_groups: dict = {"group_prefix1": ["repo_1", "repo_2"]}
    imports: List[str] = ["ubi_manifest.worker.tasks.depsolve"]
    broker_url: str = "redis://redis:6379/0"
    result_backend: str = "redis://redis:6379/0"
    ubi_manifest_data_expiration: int = (
        60 * 60 * 4
    )  # 4 hours default data expiration for redis


def make_config(celery_app):
    config_file = os.getenv("UBI_MANIFEST_CONFIG", "/etc/ubi_manifest/app.conf")
    config_from_file = configparser.ConfigParser()
    config_from_file.read(config_file)
    try:
        conf_dict = dict(config_from_file["CONFIG"])
        for conf_field in ("allowed_ubi_repo_groups", "content_config"):
            conf_item_str = config_from_file["CONFIG"].pop(conf_field) or ""
            conf_item = json.loads(re.sub(r"[\s]+", "", conf_item_str))
            conf_dict[conf_field] = conf_item

        config = Config(**conf_dict)
    except KeyError:
        config = Config()

    celery_app.config_from_object(config, force=True)
