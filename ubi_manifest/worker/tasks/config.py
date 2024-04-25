import configparser
import json
import os
import re
from typing import Any, Union

import celery
from attrs import define, field


@define
class Config:
    pulp_url: str = "some_url"
    pulp_username: str = "username"
    pulp_password: str = "pass"
    pulp_cert: str = "path/to/cert"
    pulp_key: str = "path/to/key"
    pulp_verify: Union[bool, str] = True
    content_config: dict[str, str] = {"group_prefix": "url_or_dir"}
    allowed_ubi_repo_groups: dict[str, list[str]] = {
        "group_prefix1": ["repo_1", "repo_2"]
    }
    imports: list[str] = [
        "ubi_manifest.worker.tasks.depsolve",
        "ubi_manifest.worker.tasks.repo_monitor",
    ]
    broker_url: str = "redis://redis:6379/0"
    result_backend: str = "redis://redis:6379/0"
    # 4 hours default data expiration for redis
    ubi_manifest_data_expiration: int = field(converter=int, default=60 * 60 * 4)
    publish_limit: int = field(converter=int, default=6)  # in hours
    beat_schedule: dict[str, dict[str, Any]] = {
        "monitor-repo-every-N-hours": {
            "task": "ubi_manifest.worker.tasks.repo_monitor.repo_monitor_task",
            "schedule": int(
                os.getenv("UBI_MANIFEST_REPO_MONITOR_SCHEDULE", str(60 * 60))
            ),  # in seconds
        }
    }
    timezone = "UTC"


def make_config(celery_app: celery.Celery) -> None:
    config_file = os.getenv("UBI_MANIFEST_CONFIG", "/etc/ubi_manifest/app.conf")
    config_from_file = configparser.ConfigParser()
    config_from_file.read(config_file)
    try:
        conf_dict: dict[str, Any] = dict(config_from_file["CONFIG"])
        for conf_field in ("allowed_ubi_repo_groups", "content_config"):
            conf_item_str = config_from_file["CONFIG"].pop(conf_field) or ""
            conf_item = json.loads(re.sub(r"[\s]+", "", conf_item_str))
            conf_dict[conf_field] = conf_item

        config = Config(**conf_dict)
    except KeyError:
        config = Config()

    celery_app.config_from_object(config, force=True)
