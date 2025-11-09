# backend/app/analytics/helpers.py
import dask.dataframe as dd
from app.core import config

def load_dask_df():
    path = config.PARQUET_PATH
    if not os.path.exists(path):
        raise RuntimeError("Parquet not found")
    return dd.read_parquet(path, npartitions=4)
