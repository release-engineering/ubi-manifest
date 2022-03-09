import configparser
import os
from typing import List

from attrs import define


@define
class Config:
    pulp_url: str = "some_url"
    pulp_username: str = "username"
    pulp_password: str = "pass"
    pulp_insecure: bool = False
    ubi_config_url: str = "some_url"
    allowed_ubi_repos: dict = None
    include: List[str] = ["ubi_manifest.worker.tasks.depsolve_task"]
    broker: str = "redis://localhost:6379/0"
    backend: str = "redis://localhost:6379/0"


def make_config(celery_app):
    config_file = os.getenv("UBI_MANIFEST_CONFIG", "/etc/ubi_manifest/app.conf")
    config_from_file = configparser.ConfigParser()
    config_from_file.read(config_file)
    try:
        config = Config(**dict(config_from_file["CONFIG"]))
    except KeyError:
        config = Config()

    celery_app.config_from_object(config, force=True)
