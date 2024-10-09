import configparser
import json
import os
import re
from typing import Any, Union

import celery
from attrs import AttrsInstance, define, field, validators

URL_REGEX = r"""^(?:[a-z]+:\/\/)?  # optional scheme
                (?:[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b)?  # optional main part
                (?:[-a-zA-Z0-9()@:%_\+.~#?&\/=]*)$"""
FILE_PATH_REGEX = r"^[^\x00]+$"  # allow all chars but null byte
REPO_CLASS_REGEX = r"^[A-Za-z0-9_\-\.]{1,100}$"
REPO_ID_REGEX = r"^[A-Za-z0-9_\-\.]{1,200}$"
USERNAME_REGEX = r"^\S+$"  # allow all non-white chars
PASSWORD_REGEX = r"^[^\x00]+$"  # allow all chars but null byte
TIMEZONE_REGEX = r"^[A-Za-z]{1,10}$"


def validate_content_config(_: AttrsInstance, attr: Any, value: dict[str, str]) -> None:
    for repo_class, url_or_dir in value.items():
        if not re.match(REPO_CLASS_REGEX, repo_class):
            raise ValueError(
                (
                    f"Repo classes in '{attr.name}' must match regex '{REPO_CLASS_REGEX}'."
                    f"'{repo_class}' doesn't."
                )
            )
        url_or_dir = str(url_or_dir)
        if url_or_dir.lower().startswith(("http://", "https://")):
            regex = URL_REGEX
            value_type = "Url"
        else:
            regex = FILE_PATH_REGEX
            value_type = "Path"
        if not re.match(regex, url_or_dir, re.VERBOSE):
            raise ValueError(
                f"{value_type} to config in '{attr.name}' must match regex '{regex}'."
                f"'{url_or_dir}' doesn't."
            )


def validate_repo_groups(
    _: AttrsInstance, attr: Any, value: dict[str, list[str]]
) -> None:
    for group in value.values():
        for repo in group:
            if not re.match(REPO_ID_REGEX, repo):
                raise ValueError(
                    f"Repos in '{attr.name}' must match regex '{REPO_ID_REGEX}'. '{repo}' doesn't."
                )


@define
class Config:
    pulp_url: str = field(
        validator=validators.matches_re(URL_REGEX, re.VERBOSE),
        default="https://some_url",
    )
    pulp_username: str = field(
        validator=validators.matches_re(USERNAME_REGEX), default="username"
    )
    pulp_password: str = field(
        validator=validators.matches_re(PASSWORD_REGEX), default="pass"
    )
    pulp_cert: str = field(
        validator=validators.matches_re(FILE_PATH_REGEX), default="path/to/cert"
    )
    pulp_key: str = field(
        validator=validators.matches_re(FILE_PATH_REGEX), default="path/to/key"
    )
    pulp_verify: Union[bool, str] = True
    content_config: dict[str, str] = field(
        validator=validate_content_config,
        default={"ubi": "url_or_dir_1", "client-tools": "url_or_dir_2"},
    )
    allowed_ubi_repo_groups: dict[str, list[str]] = field(
        validator=validate_repo_groups, default={}
    )
    imports: list[str] = [
        "ubi_manifest.worker.tasks.depsolve",
        "ubi_manifest.worker.tasks.repo_monitor",
        "ubi_manifest.worker.tasks.content_audit",
        "ubi_manifest.worker.tasks.celery_beat_healthcheck",
    ]
    broker_url: str = field(
        validator=validators.matches_re(URL_REGEX, re.VERBOSE),
        default="redis://redis:6379/0",
    )
    result_backend: str = field(
        validator=validators.matches_re(URL_REGEX, re.VERBOSE),
        default="redis://redis:6379/0",
    )
    # 4 hours default data expiration for redis
    ubi_manifest_data_expiration: int = field(converter=int, default=60 * 60 * 4)
    publish_limit: int = field(converter=int, default=6)  # in hours
    beat_schedule: dict[str, dict[str, Any]] = {
        "monitor-repo-every-N-hours": {
            "task": "ubi_manifest.worker.tasks.repo_monitor.repo_monitor_task",
            "schedule": int(
                os.getenv("UBI_MANIFEST_REPO_MONITOR_SCHEDULE", str(60 * 60))
            ),  # in seconds
        },
        "audit-content-every-N-hours": {
            "task": "ubi_manifest.worker.tasks.content_audit.content_audit_task",
            "schedule": int(
                os.getenv("UBI_MANIFEST_CONTENT_AUDIT_SCHEDULE", str((3 * 60) * 60))
            ),  # in seconds
        },
        "beat-healthcheck-every-N-minutes": {
            "task": "ubi_manifest.worker.tasks.celery_beat_healthcheck.beat_healthcheck_task",
            "schedule": int(
                os.getenv("UBI_MANIFEST_BEAT_HEALTHCHECK", str(60))
            ),  # in seconds
        },
    }
    timezone: str = field(
        validator=validators.matches_re(TIMEZONE_REGEX), default="UTC"
    )


def make_config(celery_app: celery.Celery) -> None:
    config_file = os.getenv("UBI_MANIFEST_CONFIG", "/etc/ubi_manifest/app.conf")
    config_from_file = configparser.ConfigParser()
    config_from_file.read(config_file)
    try:
        conf_dict: dict[str, Any] = dict(config_from_file["CONFIG"])
        for conf_field in ("allowed_ubi_repo_groups", "content_config"):
            conf_item_str = config_from_file["CONFIG"].pop(conf_field, "{}")
            conf_item = json.loads(re.sub(r"[\s]+", "", conf_item_str))
            conf_dict[conf_field] = conf_item

        config = Config(**conf_dict)
    except KeyError:
        config = Config()

    celery_app.config_from_object(config, force=True)
