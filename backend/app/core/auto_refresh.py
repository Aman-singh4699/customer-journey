# backend/app/core/auto_refresh.py
import asyncio
import traceback
from datetime import datetime
from app.core.loader import load_and_snapshot
from app.core.cache import cache

async def auto_refresh_loop(interval_minutes: int = 60):
    """Run the data loader every `interval_minutes` in background."""
    while True:
        try:
            print(f"[AUTO REFRESH] Starting data reload at {datetime.utcnow().isoformat()}")
            result = load_and_snapshot()
            print(f"[AUTO REFRESH] Completed: {result}")
            cache.set("last_refresh", datetime.utcnow().isoformat())
        except Exception as e:
            print("[AUTO REFRESH] ERROR:", e)
            traceback.print_exc()
        await asyncio.sleep(interval_minutes * 60)
