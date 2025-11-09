# backend/app/core/loader.py
import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, select
from dotenv import load_dotenv
from app.core import config
from app.core.cache import cache

load_dotenv()

# Cache keys
STATUS_KEY = "data_status"
OVERVIEW_KEY = "overview"
REV_CAT_KEY = "revenue_by_category"
MONTHLY_KEY = "monthly_revenue"
EDGES_KEY = "journey_edges"
PARQUET_KEY = "parquet_path"
LAST_PRODUCT_KEY = "last_product_by_email"

def _accumulate_dict(acc, series):
    """Accumulate sums into a dict for incremental aggregation."""
    for k, v in series.items():
        acc[k] = acc.get(k, 0) + float(v)

def _to_list_from_dict_sum(d, key_name="key", value_name="value"):
    """Convert dict to list of {key, value} for JSON serialization."""
    return [{key_name: k, value_name: v} for k, v in d.items()]

def load_and_snapshot(write_parquet: bool = True):
    """
    Chunked data loader:
    - Reads PostgreSQL table safely using SQLAlchemy reflection
    - Builds revenue aggregates & customer journey edges
    - Writes a single Parquet snapshot for fast reuse
    - Caches analytics results for API use
    """
    cache.set(STATUS_KEY, {"ready": False, "progress": 0})

    db_url = config.DATABASE_URL
    if not db_url:
        raise RuntimeError("DATABASE_URL missing in config")

    # Pandas needs sync engine (not asyncpg)
    sync_url = db_url.replace("+asyncpg", "")
    engine = create_engine(sync_url)

    chunksize = config.CHUNKSIZE
    table = config.CUSTOMER_TABLE

    # --- Reflect real columns from DB (handles case & spaces properly) ---
    metadata = MetaData()
    metadata.reflect(bind=engine, only=[table])
    tbl = metadata.tables[table]

    # Build selectable query (order by payment date if exists)
    if "Payment Date" in tbl.c:
        stmt = select(tbl).order_by(tbl.c["Payment Date"])
    elif "payment_date" in tbl.c:
        stmt = select(tbl).order_by(tbl.c["payment_date"])
    else:
        stmt = select(tbl)

    # --- Aggregation containers ---
    total_revenue = 0.0
    total_orders = 0
    rev_by_cat = {}
    monthly_rev = {}
    journey_edges = {}
    last_product_by_email = cache.get(LAST_PRODUCT_KEY, {}) or {}

    parquet_parts = []
    chunk_index = 0

    # --- Read DB in chunks safely ---
    for chunk in pd.read_sql(sql=stmt, con=engine, chunksize=chunksize):
        # Normalize column names (lowercase, remove spaces)
        chunk.columns = [c.strip().replace(" ", "_").lower() for c in chunk.columns]

        # Coerce types
        if "payment_date" in chunk.columns:
            chunk["payment_date"] = pd.to_datetime(chunk["payment_date"], errors="coerce")
        if "amount" in chunk.columns:
            chunk["amount"] = pd.to_numeric(chunk["amount"], errors="coerce").fillna(0.0)

        # --- Aggregations ---
        total_orders += len(chunk)
        total_revenue += float(chunk["amount"].sum())

        if "category" in chunk.columns:
            cat_series = chunk.groupby("category")["amount"].sum()
            _accumulate_dict(rev_by_cat, cat_series.to_dict())

        if "payment_date" in chunk.columns:
            chunk["month"] = chunk["payment_date"].dt.to_period("M").astype(str).fillna("unknown")
            mon_series = chunk.groupby("month")["amount"].sum()
            _accumulate_dict(monthly_rev, mon_series.to_dict())

        # --- Build journey edges (per customer) ---
        if "email" in chunk.columns and "product" in chunk.columns:
            chunk_sorted = chunk.sort_values(["email", "payment_date"])
            grouped = chunk_sorted.groupby("email")
            for email, g in grouped:
                products = list(g["product"].astype(str))
                # within-chunk edges
                for a, b in zip(products[:-1], products[1:]):
                    journey_edges[(a, b)] = journey_edges.get((a, b), 0) + 1
                # cross-chunk edge
                prev = last_product_by_email.get(email)
                if prev and products:
                    journey_edges[(prev, products[0])] = journey_edges.get((prev, products[0]), 0) + 1
                if products:
                    last_product_by_email[email] = products[-1]

        # --- Optionally write parquet part ---
        if write_parquet:
            os.makedirs(os.path.dirname(config.PARQUET_PATH), exist_ok=True)
            part_path = f"{config.PARQUET_PATH}.part{chunk_index}.parquet"
            chunk.reset_index(drop=True).to_parquet(part_path, index=False)
            parquet_parts.append(part_path)

        chunk_index += 1
        # Update progress
        cache.set(STATUS_KEY, {"ready": False, "progress": int(chunk_index * 100 / (chunk_index + 1))})

    # --- Combine parquet parts ---
    if write_parquet and parquet_parts:
        parts = [pd.read_parquet(p) for p in parquet_parts]
        combined = pd.concat(parts, ignore_index=True)
        combined.to_parquet(config.PARQUET_PATH, index=False)
        for p in parquet_parts:
            try:
                os.remove(p)
            except Exception:
                pass
        cache.set(PARQUET_KEY, config.PARQUET_PATH)

    # --- Store results in cache ---
    cache.set(
        OVERVIEW_KEY,
        {"total_revenue": total_revenue, "total_orders": total_orders},
        ttl_seconds=config.CACHE_TTL,
    )
    cache.set(REV_CAT_KEY, _to_list_from_dict_sum(rev_by_cat, "category", "amount"), ttl_seconds=config.CACHE_TTL)
    cache.set(MONTHLY_KEY, _to_list_from_dict_sum(monthly_rev, "month", "amount"), ttl_seconds=config.CACHE_TTL)

    edges_list = [{"source": a, "target": b, "value": v} for (a, b), v in journey_edges.items()]
    cache.set(EDGES_KEY, edges_list, ttl_seconds=config.CACHE_TTL)
    cache.set(LAST_PRODUCT_KEY, last_product_by_email, ttl_seconds=86400)
    cache.set(STATUS_KEY, {"ready": True, "progress": 100}, ttl_seconds=None)

    return {
        "rows": total_orders,
        "total_revenue": total_revenue,
        "parquet": config.PARQUET_PATH if os.path.exists(config.PARQUET_PATH) else None,
    }
