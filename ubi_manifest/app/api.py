from fastapi import APIRouter

router = APIRouter(prefix="/api/v1")


@router.get("/status")
async def status():
    return {"status": "OK"}
