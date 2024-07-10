import logging
from concurrent.futures import Future
from datetime import datetime
from typing import Union

from pubtools.pulplib import Criteria, Repository

from ubi_manifest.worker.tasks.celery import app
from ubi_manifest.worker.utils import make_pulp_client

_LOG = logging.getLogger(__name__)


@app.task  # type: ignore [misc]  # ignore untyped decorator
def repo_monitor_task() -> None:
    """
    This task runs various checks for relevant repositories.

    Currently it implements check of last_publish time of given
    repository. Repositories should be regularly published,
    if they are not, it is logged in order to easily
    investigate the incident.
    """

    to_log = {}
    with make_pulp_client(app.conf) as client:
        criteria = Criteria.with_field("ubi_population", True)
        repos = client.search_repository(criteria)
        for repo in repos:
            result = _check_last_publish(repo)

            if result:
                to_log[repo.id] = result

    _log_findigs(to_log)


def _check_last_publish(repository: Future[Repository]) -> Union[str, None]:
    out = None
    limit = app.conf.publish_limit
    current_time = datetime.now()
    for distributor in repository.distributors:  # type: ignore
        if distributor.is_rsync:
            time_diff = current_time - distributor.last_publish
            td_hrs = time_diff.days * 24 * 60 + time_diff.seconds / 3600
            _LOG.debug(
                "Last publish check: %s, last_publish: %s, diff: %.2f",
                repository.id,  # type: ignore
                distributor.last_publish,
                td_hrs,
            )

            if td_hrs > limit:
                out = (
                    f"Repository {repository.id} hasn't been published within "  # type: ignore
                    f"alloted time limit: {limit} hrs, unpublished for {td_hrs:.2f} hrs"
                )
    return out


def _log_findigs(data: dict[str, str]) -> None:
    for msg in data.values():
        _LOG.warning(msg)
