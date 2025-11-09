# backend/app/routes/journey.py
from fastapi import APIRouter, HTTPException
from app.core.cache import cache
from app.core.loader import EDGES_KEY, LAST_PRODUCT_KEY, PARQUET_KEY
import pandas as pd
import os

router = APIRouter(prefix="/journey", tags=["Journey"])

@router.get("/{email}")
def get_customer_journey(email: str):
    """Return sequence of products purchased by a specific customer."""
    parquet_path = cache.get(PARQUET_KEY)
    if not parquet_path or not os.path.exists(parquet_path):
        raise HTTPException(status_code=503, detail="Data not loaded yet. Try again later.")

    df = pd.read_parquet(parquet_path, columns=["email", "product", "payment_date"])
    df["email"] = df["email"].astype(str).str.lower()
    email = email.lower()
    user_df = df[df["email"] == email].sort_values("payment_date")

    if user_df.empty:
        raise HTTPException(status_code=404, detail=f"No purchases found for {email}")

    products = list(user_df["product"].dropna())
    return {
        "email": email,
        "count": len(products),
        "sequence": products,
    }

@router.get("/edges")
def journey_edges():
    """Return Sankey-style edges (aggregated product transitions)."""
    edges = cache.get(EDGES_KEY)
    if not edges:
        raise HTTPException(status_code=503, detail="Journey data not available yet.")
    return edges
