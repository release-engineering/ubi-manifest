import configparser
import os
from unittest.mock import patch

import pytest
from celery import Celery

from ubi_manifest.worker.tasks.config import make_config

TEST_CONF_FILE = os.path.join(os.path.dirname(__file__), "data/conf/test.conf")


def test_make_config():
    with patch.dict(os.environ, {"UBI_MANIFEST_CONFIG": TEST_CONF_FILE}):
        celery_app = Celery()
        make_config(celery_app)

        # we can get value by attr
        assert celery_app.conf.pulp_url == "https://foo-bar.pulp.com/"
        # and also by key
        assert celery_app.conf["pulp_url"] == "https://foo-bar.pulp.com/"
        # let's assert other keys
        assert celery_app.conf["pulp_username"] == "xxx"
        assert celery_app.conf["pulp_password"] == "yyy"
        assert celery_app.conf["pulp_cert"] == "path/to/pulp_cert"
        assert celery_app.conf["pulp_key"] == "path/to/pulp_key"
        assert celery_app.conf["content_config"] == {
            "ubi": "https://gitlab.foo.bar.com/ubi-config",
            "client-tools": "https://gitlab.foo.bar.com/ct-config",
        }
        assert celery_app.conf["allowed_ubi_repo_groups"] == {}

        # check properly converted fields to int types
        assert celery_app.conf["publish_limit"] == 2
        assert celery_app.conf["ubi_manifest_data_expiration"] == 4444


@pytest.mark.parametrize(
    "option,value",
    [
        ("pulp_username", "fo o"),
        ("content_config", '{"ubi": "https://ubi..!!"}'),
        ("content_config", '{"ubi??": "https://ubi"}'),
        ("allowed_ubi_repo_groups", '{"ubiX:test": ["repo??"]}'),
    ],
)
def test_config_wrong_attributes(option, value):
    config_from_file = configparser.ConfigParser()
    config_from_file.read(TEST_CONF_FILE)
    config_from_file.set("CONFIG", option, value)

    with patch("ubi_manifest.worker.tasks.config.configparser.ConfigParser") as config:
        config.return_value = config_from_file
        celery_app = Celery()
        with pytest.raises(ValueError, match=f".*{option}.*must match regex.*"):
            make_config(celery_app)
