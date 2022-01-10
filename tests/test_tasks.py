from ubi_manifest.worker.tasks.dummy_task import dummy_task


def test_dummy_task():
    result = dummy_task()
    assert result == "CELERY_OK"
