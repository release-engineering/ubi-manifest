import celery

from ubi_manifest.worker.tasks.config import make_config

app = celery.Celery()
make_config(app)
