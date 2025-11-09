# backend/app/routes/data.py
from fastapi import APIRouter, BackgroundTasks, HTTPException
from app.core import config
from app.core.cache import cache
from app.core.loader import load_and_snapshot, STATUS_KEY
import pandas as pd
from sqlalchemy import create_engine
import os

router = APIRouter(prefix="/data", tags=["Data"])

@router.get("/rows")
def get_rows(offset: int = 0, limit: int = 100):
    if limit > 1000:
        raise HTTPException(status_code=400, detail="limit max 1000")

    parquet = cache.get("parquet_path") or (config.PARQUET_PATH if os.path.exists(config.PARQUET_PATH) else None)
    if parquet and os.path.exists(parquet):
        df = pd.read_parquet(parquet, columns=None)
        slice_df = df.iloc[offset: offset + limit]
        return slice_df.to_dict(orient="records")
    # fallback to DB
    db = config.DATABASE_URL
    if not db:
        raise HTTPException(status_code=500, detail="No data source available")
    sync_db = db.replace("+asyncpg", "")
    engine = create_engine(sync_db)
    table = config.CUSTOMER_TABLE
    sql = f'SELECT Category, Product, Name, Email, Phone, "Payment Date" as payment_date, Amount FROM {table} ORDER BY "Payment Date" DESC NULLS LAST LIMIT {limit} OFFSET {offset}'
    with engine.connect() as conn:
        df = pd.read_sql(sql, con=conn)
    return df.to_dict(orient="records")

@router.post("/refresh")
def refresh(background_tasks: BackgroundTasks):
    # schedule background reload (not blocking)
    background_tasks.add_task(load_and_snapshot)
    # mark status
    cache.set(STATUS_KEY, {"ready": False, "progress": 0}, ttl_seconds=None)
    return {"status": "scheduled"}
