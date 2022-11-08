# **ubi-manifest**
[![Build Status](https://github.com/release-engineering/ubi-manifest/actions/workflows/tox-test.yml/badge.svg)](https://github.com/release-engineering/ubi-manifest/actions/workflows/tox-test.yml)
[![codecov](https://codecov.io/gh/release-engineering/ubi-manifest/branch/master/graph/badge.svg?token=EILYTN2NON)](https://codecov.io/gh/release-engineering/ubi-manifest)
[![Source](https://badgen.net/badge/icon/source?icon=github&label)](https://github.com/release-engineering/ubi-manifest/)

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

Dev-env setup:
--------------

For running ubi-manifest related containers one can use provided docker-compose file.
For succesfull running of celery tasks, it's required to properly update the config file ./conf/app.conf
with credentials to pulp and gitlab repository with ubi-config files.
There are certs prepared in ./conf/certs/ for accessing dependent services, 
if any different certs are required, copy them to the directory.

Then podman-compose can be used for building and running the service:
```
podman-compose build
podman-compose up -d
```
Service should be available at 127.0.0.0:8000.

For removing containers:
```
podman-compose down
```

License
-------

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.
