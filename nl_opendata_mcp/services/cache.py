"""
Cache management for nl-opendata-mcp server.

This module provides TTL-based caching with persistence support for:
- CBS catalog data (4,800+ datasets)
- Downloaded dataset metadata

Features:
    - Automatic expiration based on TTL (default: 24 hours)
    - Persistence to JSON files
    - Lazy loading from disk
    - Statistics and monitoring

Classes:
    CatalogCache: Manages the CBS dataset catalog cache
    DatasetCache: Tracks downloaded datasets and their locations

Example:
    >>> from nl_opendata_mcp.services import catalog_cache, dataset_cache
    >>>
    >>> # Check if catalog needs refresh
    >>> if catalog_cache.is_expired:
    >>>     await fetch_catalog()
    >>>
    >>> # Check if dataset is already downloaded
    >>> if dataset_cache.exists("/path/to/file.csv"):
    >>>     print("Using cached file")
"""
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Optional, TypeVar, Generic, Any
from dataclasses import dataclass

from ..config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

T = TypeVar('T')


@dataclass
class CacheEntry:
    """A single cache entry with expiration tracking."""
    data: Any
    created_at: str
    expires_at: str

    @classmethod
    def create(cls, data: Any, ttl_hours: int = 24) -> 'CacheEntry':
        """Create a new cache entry with TTL."""
        now = datetime.now()
        return cls(
            data=data,
            created_at=now.isoformat(),
            expires_at=(now + timedelta(hours=ttl_hours)).isoformat()
        )

    @property
    def is_expired(self) -> bool:
        """Check if this entry has expired."""
        return datetime.now() > datetime.fromisoformat(self.expires_at)

    @property
    def age_hours(self) -> float:
        """Get the age of this entry in hours."""
        created = datetime.fromisoformat(self.created_at)
        return (datetime.now() - created).total_seconds() / 3600


class CatalogCache:
    """
    Cache manager for CBS catalog data with TTL support.

    Features:
    - Automatic expiration based on TTL
    - Persistence to JSON file
    - Lazy loading from disk
    - Thread-safe operations
    """

    def __init__(
        self,
        cache_file: str = None,
        ttl_hours: int = 24
    ):
        self.cache_file = cache_file or settings.cache_file
        self.ttl_hours = ttl_hours
        self._data: list = []
        self._metadata: Optional[dict] = None
        self._loaded = False

    def _load_from_disk(self) -> bool:
        """Load cache from disk if available."""
        if not os.path.exists(self.cache_file):
            return False

        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                content = json.load(f)

            # Handle both old format (plain list) and new format (with metadata)
            if isinstance(content, dict) and 'data' in content:
                self._data = content['data']
                self._metadata = content.get('metadata', {})

                # Check if cache is expired
                expires_at = self._metadata.get('expires_at')
                if expires_at:
                    if datetime.now() > datetime.fromisoformat(expires_at):
                        logger.info("Catalog cache expired, will refresh")
                        return False
            else:
                # Old format - just a list, check file modification time
                self._data = content
                self._metadata = None

                # Check file age for old format
                file_stat = os.stat(self.cache_file)
                age_hours = (datetime.now().timestamp() - file_stat.st_mtime) / 3600
                if age_hours > self.ttl_hours:
                    logger.info(f"Catalog cache too old ({age_hours:.1f}h), will refresh")
                    return False

            self._loaded = True
            logger.info(f"Loaded {len(self._data)} datasets from cache")
            return True

        except Exception as e:
            logger.error(f"Failed to load cache from disk: {e}")
            return False

    def _save_to_disk(self) -> bool:
        """Save cache to disk with metadata."""
        try:
            content = {
                'data': self._data,
                'metadata': {
                    'created_at': datetime.now().isoformat(),
                    'expires_at': (datetime.now() + timedelta(hours=self.ttl_hours)).isoformat(),
                    'count': len(self._data),
                    'ttl_hours': self.ttl_hours
                }
            }
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(content, f)
            logger.info(f"Saved {len(self._data)} datasets to cache")
            return True
        except Exception as e:
            logger.error(f"Failed to save cache to disk: {e}")
            return False

    @property
    def data(self) -> list:
        """Get cached data, loading from disk if needed."""
        if not self._loaded:
            self._load_from_disk()
        return self._data

    @data.setter
    def data(self, value: list):
        """Set cache data and persist to disk."""
        self._data = value
        self._loaded = True
        self._save_to_disk()

    @property
    def is_loaded(self) -> bool:
        """Check if cache has been loaded."""
        return self._loaded and len(self._data) > 0

    @property
    def is_expired(self) -> bool:
        """Check if cache is expired."""
        if not self._loaded:
            self._load_from_disk()

        if self._metadata and 'expires_at' in self._metadata:
            return datetime.now() > datetime.fromisoformat(self._metadata['expires_at'])

        # For old format or missing metadata, check file age
        if os.path.exists(self.cache_file):
            file_stat = os.stat(self.cache_file)
            age_hours = (datetime.now().timestamp() - file_stat.st_mtime) / 3600
            return age_hours > self.ttl_hours

        return True

    @property
    def age_hours(self) -> Optional[float]:
        """Get cache age in hours."""
        if self._metadata and 'created_at' in self._metadata:
            created = datetime.fromisoformat(self._metadata['created_at'])
            return (datetime.now() - created).total_seconds() / 3600

        if os.path.exists(self.cache_file):
            file_stat = os.stat(self.cache_file)
            return (datetime.now().timestamp() - file_stat.st_mtime) / 3600

        return None

    def clear(self):
        """Clear the cache."""
        self._data = []
        self._metadata = None
        self._loaded = False
        if os.path.exists(self.cache_file):
            os.remove(self.cache_file)
            logger.info("Cache cleared")

    def get_stats(self) -> dict:
        """Get cache statistics."""
        return {
            'loaded': self._loaded,
            'count': len(self._data),
            'expired': self.is_expired,
            'age_hours': self.age_hours,
            'ttl_hours': self.ttl_hours,
            'file_exists': os.path.exists(self.cache_file)
        }


class DatasetCache:
    """
    Cache manager for downloaded dataset metadata.

    Tracks which datasets have been downloaded and their locations.
    """

    def __init__(self, cache_file: str = None):
        self.cache_file = cache_file or settings.dataset_cache_file
        self._data: dict = {}
        self._loaded = False

    def _load_from_disk(self) -> bool:
        """Load cache from disk."""
        if not os.path.exists(self.cache_file):
            return False

        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                self._data = json.load(f)
            self._loaded = True
            logger.debug(f"Loaded {len(self._data)} dataset entries from cache")
            return True
        except Exception as e:
            logger.error(f"Failed to load dataset cache: {e}")
            return False

    def _save_to_disk(self) -> bool:
        """Save cache to disk."""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self._data, f, indent=2)
            logger.debug(f"Saved {len(self._data)} dataset entries to cache")
            return True
        except Exception as e:
            logger.error(f"Failed to save dataset cache: {e}")
            return False

    def get(self, path: str) -> Optional[dict]:
        """Get cached dataset info by file path."""
        if not self._loaded:
            self._load_from_disk()
        return self._data.get(path)

    def set(self, path: str, dataset_id: str, records: int):
        """Cache dataset info."""
        if not self._loaded:
            self._load_from_disk()

        self._data[path] = {
            'dataset_id': dataset_id,
            'records': records,
            'timestamp': datetime.now().isoformat()
        }
        self._save_to_disk()

    def remove(self, path: str):
        """Remove a cached entry."""
        if not self._loaded:
            self._load_from_disk()

        if path in self._data:
            del self._data[path]
            self._save_to_disk()

    def exists(self, path: str) -> bool:
        """Check if a dataset is cached and file exists."""
        entry = self.get(path)
        if entry and os.path.exists(path):
            return True
        elif entry:
            # File no longer exists, remove from cache
            self.remove(path)
        return False

    @property
    def entries(self) -> dict:
        """Get all cache entries."""
        if not self._loaded:
            self._load_from_disk()
        return self._data.copy()

    def clear(self):
        """Clear the cache."""
        self._data = {}
        self._loaded = False
        if os.path.exists(self.cache_file):
            os.remove(self.cache_file)
            logger.info("Dataset cache cleared")


# Global cache instances
catalog_cache = CatalogCache(ttl_hours=24)
dataset_cache = DatasetCache()
