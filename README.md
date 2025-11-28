# **ubi-manifest**
[![Build Status](https://github.com/release-engineering/ubi-manifest/actions/workflows/tox-test.yml/badge.svg)](https://github.com/release-engineering/ubi-manifest/actions/workflows/tox-test.yml)
[![codecov](https://codecov.io/gh/release-engineering/ubi-manifest/branch/master/graph/badge.svg?token=EILYTN2NON)](https://codecov.io/gh/release-engineering/ubi-manifest)
[![Source](https://badgen.net/badge/icon/source?icon=github&label)](https://github.com/release-engineering/ubi-manifest/)
[![Documentation](https://img.shields.io/website?label=docs&url=https%3A%2F%2Frelease-engineering.github.io%2Fubi-manifest%2F)](https://release-engineering.github.io/ubi-manifest/)

A Flask-based service used by [release-engineering](https://github.com/release-engineering) for resolving manifests of ubi content.

- [Documentation](https://release-engineering.github.io/ubi-manifest/)
 
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
For successful running of celery tasks, it's required to properly update the config file ./conf/app.conf
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

### Spoofing authentication

The ubi-manifest service uses a reverse proxy provided by [platform-sidecar](https://gitlab.corp.redhat.com/it-platform/platform-sidecar), which adds
a special `X-RhApiPlatform-CallContext` header to all incoming requests. This header contains a
base64-encoded form of the following JSON object:
```
{
  "client": {
    "roles": ["someRole", "anotherRole"],
    "authenticated": true,
    "serviceAccountId": "clientappname"
  },
  "user": {
    "roles": ["reader"],
    "authenticated": true,
    "internalUsername": "someuser"
  }
}
```
The roles and authenticated fields influence whether a request will be permitted.

Currently ubi-manifest uses two roles:
- **creator** - submitting requests for manifest creation
- **reader** - retrieving created manifests and task states

Roles are assigned to users/services according to the current LDAP groups.

However, when running the service as described above, there is no platform sidecar available to
handle the authentication and authorization. Therefore, during development, arbitrary values for
the `X-RhApiPlatform-CallContext` header can be used to provide the required auth information.
Due to the format of this header, generating these values by hand can be cumbersome.

To assist with this, a helper script is provided in the ubi-manifest repo at `scripts/call-context`.
This script accepts any number of role names as arguments and produces a header value which will
produce an authenticated & authorized request using those roles.

For example, if we want to use curl to make a request to an endpoint which requires a `creator`
role, we can use the following command:
```
   curl \
     -v POST --json '{"repo_ids": ["some_repo1", "some_repo2"]}' \
     -H "X-RhApiPlatform-CallContext: $(scripts/call-context creator)" \
     http://127.0.0.1:8000/api/v1/manifest
```

License
-------

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

