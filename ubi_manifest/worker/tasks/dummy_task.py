from ubi_manifest.worker.tasks.celery import app


@app.task
def dummy_task():
    # this was created for initial repo setup and will be removed
    # as implementation goes on
    return "CELERY_OK"
