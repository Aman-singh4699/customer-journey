# backend/app/core/cache.py
from datetime import datetime, timedelta
from threading import Lock

class SimpleCache:
    def __init__(self):
        self._lock = Lock()
        self._store = {}

    def set(self, key, value, ttl_seconds: int = None):
        expire = (datetime.utcnow() + timedelta(seconds=ttl_seconds)) if ttl_seconds else None
        with self._lock:
            self._store[key] = {"value": value, "expire": expire}

    def get(self, key, default=None):
        with self._lock:
            entry = self._store.get(key)
            if not entry:
                return default
            if entry["expire"] and datetime.utcnow() > entry["expire"]:
                del self._store[key]
                return default
            return entry["value"]

    def delete(self, key):
        with self._lock:
            self._store.pop(key, None)

    def clear(self):
        with self._lock:
            self._store.clear()

# singleton
cache = SimpleCache()
