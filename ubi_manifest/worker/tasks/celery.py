import celery
from typing import Any
from ubi_manifest.worker.tasks.config import make_config

app = celery.Celery()
make_config(app)


@celery.signals.celeryd_init.connect  # type: ignore [misc]  # ignore untyped decorator
def setup_log_format(conf: Any, **_kwargs: Any) -> None:  # pragma: no cover
    conf.worker_log_format = (
        "%(asctime)s - [%(levelname)s] - [%(name)s/%(process)d] - %(message)s"
    )
