import configparser
import json
import os
import re
from typing import List

from attrs import define


@define
class Config:
    pulp_url: str = "some_url"
    pulp_username: str = "username"
    pulp_password: str = "pass"
    pulp_insecure: bool = False
    ubi_config_url: str = "some_url"
    allowed_ubi_repo_groups: dict = {"group1": ["repo_1", "repo_2"]}
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
        repo_groups_str = (
            config_from_file["CONFIG"].pop("allowed_ubi_repo_groups") or ""
        )
        repo_groups_str = re.sub(r"[\s]+", "", repo_groups_str)
        repo_groups = json.loads(repo_groups_str)
        conf_dict = dict(config_from_file["CONFIG"])
        conf_dict["allowed_ubi_repo_groups"] = repo_groups
        config = Config(**conf_dict)
    except KeyError:
        config = Config()

    celery_app.config_from_object(config, force=True)
