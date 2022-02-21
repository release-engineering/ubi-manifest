import os
import celery


def celery_init():
    return celery.Celery(
        broker=os.getenv("CELERY_BROKER", "redis://localhost:6379/0"),
        backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0"),
        include=["ubi_manifest.worker.tasks.dummy_task"],
    )


app = celery_init()
