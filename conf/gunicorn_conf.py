# Export on 8000, which is exposed by the container image
bind = "0.0.0.0:8000"

worker_class = "uvicorn.workers.UvicornWorker"

workers = 4

logconfig = "/src/conf/logging.ini"
