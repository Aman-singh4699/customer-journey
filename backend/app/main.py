# backend/app/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes import analytics, journey, data
from app.core.auto_refresh import auto_refresh_loop
import asyncio

app = FastAPI(title="Customer Journey API", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# register routers
app.include_router(analytics.router)
app.include_router(journey.router)
app.include_router(data.router)

@app.on_event("startup")
async def startup_event():
    """Start background auto-refresh task."""
    asyncio.create_task(auto_refresh_loop(interval_minutes=60))  # refresh hourly

@app.get("/")
def root():
    return {"status": "ok", "message": "Customer Journey API", "docs": "/docs"}

@app.get("/health")
def health():
    return {"status": "ok"}
