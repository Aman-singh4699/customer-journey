# backend/app/core/config.py
import os
from dotenv import load_dotenv
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
CUSTOMER_TABLE = os.getenv("CUSTOMER_TABLE", "customer_journey")
PARQUET_PATH = os.getenv("PARQUET_PATH", "backend/data/customer_journey.parquet")
CHUNKSIZE = int(os.getenv("CHUNKSIZE", "20000"))
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))
