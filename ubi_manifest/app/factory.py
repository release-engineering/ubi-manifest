from fastapi import FastAPI

from ubi_manifest.app import api


def create_app() -> FastAPI:
    app = FastAPI()
    app.include_router(api.router)

    return app
