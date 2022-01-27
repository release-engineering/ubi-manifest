ubi-manifest
============

A Flask-based service used by [release-engineering](https://github.com/release-engineering) for resolving manifests of ubi content.
 
Installation
------------
TODO - implementation in progress
  
Development
-----------
Patches may be contributed via pull requests to
https://github.com/release-engineering/ubi-manifest.

All changes must pass the automated test suite, along with various static
checks.

The [Black](https://black.readthedocs.io/) code style is enforced.
Enabling autoformatting via a pre-commit hook is recommended:

```
pip install -r requirements-dev.txt
pre-commit install
```

 - Setup and run fastapi app:
 - In py3 virtual env do:

```
pip install .
export CELERY_BROKER=redis://<ip_of_localhost>:6379/0
export CELERY_RESULT_BACKEND=redis://<ip_of_localhost>:6379/0

uvicorn ubi_manifest.app.factory:create_app --factory --reload 
```

- Build and run redis in container:

```
podman build . -t redis -f docker/Dockerfile-broker
run -p 6379:6379 -d redis
```

- Build and run celery worker in container:

```
podman build . -t ubi-manifest-workers -f docker/Dockerfile-workers
podman run --env CELERY_BROKER --env CELERY_RESULT_BACKEND -d ubi-manifest-workers 
```

License
-------

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
