# backend/app/routes/analytics.py
from fastapi import APIRouter
from app.core.cache import cache
from app.core.loader import OVERVIEW_KEY, REV_CAT_KEY, MONTHLY_KEY, STATUS_KEY

router = APIRouter(prefix="/analytics", tags=["Analytics"])

@router.get("/overview")
def overview():
    status = cache.get(STATUS_KEY, {"ready": False})
    if not status.get("ready"):
        return {
            "status": "loading",
            "message": "Data is being prepared. Please try again shortly.",
            "progress": status.get("progress", 0)
        }
    overview = cache.get(OVERVIEW_KEY)
    return overview or {"status": "empty"}

@router.get("/revenue")
def revenue():
    status = cache.get(STATUS_KEY, {"ready": False})
    if not status.get("ready"):
        return {"status": "loading", "message": "Data is being prepared."}
    cats = cache.get(REV_CAT_KEY, [])
    monthly = cache.get(MONTHLY_KEY, [])
    return {"by_category": cats, "monthly": monthly}
