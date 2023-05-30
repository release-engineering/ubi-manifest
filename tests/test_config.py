import os
from unittest import mock

from celery import Celery

from ubi_manifest.worker.tasks.config import make_config

TEST_CONF_FILE = os.path.join(os.path.dirname(__file__), "data/conf/test.conf")


def test_make_config():
    with mock.patch.dict(os.environ, {"UBI_MANIFEST_CONFIG": TEST_CONF_FILE}):
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
        assert (
            celery_app.conf["ubi_config_url"] == "https://gitlab.foo.bar.com/ubi-config"
        )
        assert celery_app.conf["allowed_ubi_repo_groups"] == {
            "ubiX:test": ["repo_1", "repo_2", "repo_3"]
        }
