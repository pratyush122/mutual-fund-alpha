import json
import os
from typing import Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class Cache:
    """Simple file-based cache with TTL support."""

    def __init__(self, cache_dir: str = "data/cache/", ttl_hours: int = 24):
        """
        Initialize cache.

        Args:
            cache_dir: Directory to store cache files
            ttl_hours: Time to live in hours
        """
        self.cache_dir = cache_dir
        self.ttl = timedelta(hours=ttl_hours)

        # Create cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_cache_path(self, key: str) -> str:
        """Get the file path for a cache key."""
        return os.path.join(self.cache_dir, f"{key}.json")

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired
        """
        cache_path = self._get_cache_path(key)

        if not os.path.exists(cache_path):
            return None

        try:
            with open(cache_path, "r") as f:
                data = json.load(f)

            # Check if cache is expired
            cached_time = datetime.fromisoformat(data["timestamp"])
            if datetime.now() - cached_time > self.ttl:
                logger.info(f"Cache expired for key: {key}")
                os.remove(cache_path)
                return None

            logger.info(f"Cache hit for key: {key}")
            return data["value"]
        except Exception as e:
            logger.warning(f"Failed to read cache for key {key}: {e}")
            return None

    def set(self, key: str, value: Any) -> None:
        """
        Store value in cache.

        Args:
            key: Cache key
            value: Value to cache
        """
        cache_path = self._get_cache_path(key)
        data = {
            "timestamp": datetime.now().isoformat(),
            "value": value
        }

        try:
            with open(cache_path, "w") as f:
                json.dump(data, f)
            logger.info(f"Cache set for key: {key}")
        except Exception as e:
            logger.warning(f"Failed to write cache for key {key}: {e}")

    def clear(self, key: str) -> None:
        """
        Remove a specific key from cache.

        Args:
            key: Cache key to remove
        """
        cache_path = self._get_cache_path(key)
        if os.path.exists(cache_path):
            os.remove(cache_path)
            logger.info(f"Cache cleared for key: {key}")

    def clear_all(self) -> None:
        """Clear all cache entries."""
        for filename in os.listdir(self.cache_dir):
            file_path = os.path.join(self.cache_dir, filename)
            try:
                os.remove(file_path)
            except Exception as e:
                logger.warning(f"Failed to remove cache file {filename}: {e}")
        logger.info("All cache cleared")