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

CI/CD 
-----------
After a PR is merged, new ubi-manifest-app image is built and pushed to [Quay](https://quay.io/repository/redhat-user-workloads/ubi-manifest-tenant/ubi-manifest-app) via build pipelines under `.tekton` directory.
This image is tracked in [ubi-manifest-workflows](https://gitlab.cee.redhat.com/exd-guild-ubipop/ubi-manifest-workflows) repo in `latest-quay-image.yaml` file and regularly updated via renovate. The renovate's MR with the updated image digest triggers the downstream Konflux pipelines, defined in that repo. These pipelines then build, test and release the new code automatically.


Dev-env setup:
--------------

It is also possible to build and run ubi-manifest service locally, using provided `docker-compose.yml` and docker files under `dev-docker/` directory.
You also need to update the config file `./conf/app.conf`:
- `pulp_url`: URL to rhsm-pulp instance you want to use
- `content_config`: dictionary of repo group and URL with respective config files
- `pulp_<cert|key>`: path to rhsm-pulp cert and key

OR
- `pulp_<username|password>` rhsm-pulp username and password

**Note**: If you use pulp cert and key, you need to copy them into `./conf/` directory 
and update the `dev-docker/Dockerfile-app` and `dev-docker/Dockerfile-worker` to also COPY these certs into the build context. The path where the certs are copied must be the same as you put in the `./conf/app.conf`:
```
COPY ./conf/my-pulp.crt /etc/ubi_manifest/my-pulp.crt
COPY ./conf/my-pulp.key /etc/ubi_manifest/my-pulp.key
```

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
