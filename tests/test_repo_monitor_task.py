import logging
from datetime import datetime, timedelta
from unittest import mock

from pubtools.pulplib import Distributor

from ubi_manifest.worker.tasks import repo_monitor
from ubi_manifest.worker.tasks.celery import app

from .utils import create_and_insert_repo


def test_repo_monitor_task(pulp, caplog):
    publish_time_late = datetime.now() - timedelta(
        seconds=(app.conf.publish_limit + 1) * 60 * 60
    )
    publish_time_ok = datetime.now() - timedelta(
        seconds=(app.conf.publish_limit - 1) * 60 * 60
    )

    _ = create_and_insert_repo(
        id="rhel_repo_late_publish",
        pulp=pulp,
        content_set="cs_rpm_in",
        relative_url="foo/rhel1/os",
        ubi_population=True,
        distributors=[
            Distributor(
                id="cdn_distributor",
                type_id="rpm_rsync_distributor",
                repo_id="rhel_repo_late_publish",
                relative_url="foo/rhel1/os",
                last_publish=publish_time_late,
            )
        ],
    )

    _ = create_and_insert_repo(
        id="rhel_repo_ok_publish",
        pulp=pulp,
        content_set="cs_rpm_in",
        relative_url="foo/rhel2/os",
        ubi_population=True,
        distributors=[
            Distributor(
                id="cdn_distributor",
                type_id="rpm_rsync_distributor",
                repo_id="rhel_repo_ok_publish",
                relative_url="foo/rhel2/os",
                last_publish=publish_time_ok,
            )
        ],
    )

    with caplog.at_level(
        logging.DEBUG, logger="ubi_manifest.worker.tasks.repo_monitor"
    ):
        with mock.patch("ubi_manifest.worker.utils.Client") as client:
            client.return_value = pulp.client
            # let run the repo_monitor task
            result = repo_monitor.repo_monitor_task()

            # result is expected to be None
            assert result is None

        expected_logs = [
            f"Last publish check: rhel_repo_late_publish, last_publish: {publish_time_late}, diff: 7.00",
            f"Last publish check: rhel_repo_ok_publish, last_publish: {publish_time_ok}, diff: 5.00",
            "Repository rhel_repo_late_publish hasn't been published within alloted time limit: 6 hrs, unpublished for 7.00 hrs",
        ]

        # check logs
        assert len(caplog.messages) == 3
        for real_msg, expected_msg in zip(sorted(caplog.messages), expected_logs):
            assert expected_msg in real_msg
