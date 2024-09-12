import json
from datetime import datetime
from unittest.mock import patch

from ubi_manifest.worker.tasks import celery_beat_healthcheck

from .utils import MockedRedis


@patch("ubi_manifest.worker.tasks.celery_beat_healthcheck.datetime")
@patch("ubi_manifest.worker.tasks.celery_beat_healthcheck.redis.from_url")
def test_beat_healthcheck_task(mock_redis, mock_datetime):
    redis = MockedRedis(data={})
    mock_redis.return_value = redis
    mock_datetime.now.return_value = datetime(2024, 8, 27, 11, 22, 56, 961242)

    celery_beat_healthcheck.beat_healthcheck_task()

    result = redis.get("celery-beat-heartbeat")
    assert result == "2024-08-27T11:22:56.961242"
