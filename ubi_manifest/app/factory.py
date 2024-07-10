from fastapi import Depends, FastAPI

from ubi_manifest.app import api

from ..auth import log_login


def create_app() -> FastAPI:
    app = FastAPI(dependencies=[Depends(log_login)])
    app.include_router(api.router)

    return app
