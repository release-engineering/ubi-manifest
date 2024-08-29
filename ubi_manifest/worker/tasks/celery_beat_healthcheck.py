from datetime import datetime

import redis

from ubi_manifest.worker.tasks.celery import app


@app.task  # type: ignore [misc]  # ignore untyped decorator
def beat_healthcheck_task() -> None:
    """
    This task updates 'celery-beat-heartbeat' value in redis with current time every minute.
    It is used for healthcheck of the celery beat schedule.
    """
    redis_client = redis.from_url(app.conf.result_backend)
    redis_client.set("celery-beat-heartbeat", datetime.now().isoformat())
