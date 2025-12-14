
"""
Tests for cache functionality.
"""
import pytest
import sys
import os
import json
import tempfile
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nl_opendata_mcp.services.cache import CatalogCache, DatasetCache, CacheEntry


class TestCacheEntry:
    """Tests for CacheEntry class."""

    def test_create_entry(self):
        """Test creating a cache entry."""
        entry = CacheEntry.create({"test": "data"}, ttl_hours=24)
        assert entry.data == {"test": "data"}
        assert not entry.is_expired

    def test_expired_entry(self):
        """Test expired entry detection."""
        past = datetime.now() - timedelta(hours=1)
        entry = CacheEntry(
            data={"test": "data"},
            created_at=(past - timedelta(hours=25)).isoformat(),
            expires_at=past.isoformat()
        )
        assert entry.is_expired

    def test_age_calculation(self):
        """Test age calculation."""
        entry = CacheEntry.create({"test": "data"}, ttl_hours=24)
        assert entry.age_hours < 1  # Just created


class TestCatalogCache:
    """Tests for CatalogCache class."""

    def test_initial_state(self):
        """Test initial cache state."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            cache_file = f.name

        try:
            # Remove the file to start clean
            os.remove(cache_file)
            cache = CatalogCache(cache_file=cache_file, ttl_hours=24)
            assert not cache.is_loaded
            assert cache.data == []
        finally:
            if os.path.exists(cache_file):
                os.remove(cache_file)

    def test_save_and_load(self):
        """Test saving and loading cache."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            cache_file = f.name

        try:
            # Create and save
            cache1 = CatalogCache(cache_file=cache_file, ttl_hours=24)
            cache1.data = [{"id": "1"}, {"id": "2"}]

            # Load in new instance
            cache2 = CatalogCache(cache_file=cache_file, ttl_hours=24)
            assert len(cache2.data) == 2
            assert cache2.data[0]["id"] == "1"
        finally:
            if os.path.exists(cache_file):
                os.remove(cache_file)

    def test_clear_cache(self):
        """Test clearing cache."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            cache_file = f.name

        try:
            cache = CatalogCache(cache_file=cache_file, ttl_hours=24)
            cache.data = [{"id": "1"}]
            cache.clear()

            assert cache.data == []
            assert not os.path.exists(cache_file)
        finally:
            if os.path.exists(cache_file):
                os.remove(cache_file)

    def test_stats(self):
        """Test cache statistics."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            cache_file = f.name

        try:
            cache = CatalogCache(cache_file=cache_file, ttl_hours=24)
            cache.data = [{"id": "1"}, {"id": "2"}]

            stats = cache.get_stats()
            assert stats["count"] == 2
            assert stats["ttl_hours"] == 24
            assert stats["loaded"] == True
        finally:
            if os.path.exists(cache_file):
                os.remove(cache_file)


class TestDatasetCache:
    """Tests for DatasetCache class."""

    def test_set_and_get(self):
        """Test setting and getting cache entries."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            cache_file = f.name

        try:
            cache = DatasetCache(cache_file=cache_file)
            cache.set("/path/to/file.csv", "85313NED", 1000)

            entry = cache.get("/path/to/file.csv")
            assert entry is not None
            assert entry["dataset_id"] == "85313NED"
            assert entry["records"] == 1000
        finally:
            if os.path.exists(cache_file):
                os.remove(cache_file)

    def test_exists_with_file(self):
        """Test exists check when file exists."""
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            file_path = f.name
            f.write(b"test")

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            cache_file = f.name

        try:
            cache = DatasetCache(cache_file=cache_file)
            cache.set(file_path, "85313NED", 100)

            assert cache.exists(file_path) == True
        finally:
            if os.path.exists(file_path):
                os.remove(file_path)
            if os.path.exists(cache_file):
                os.remove(cache_file)

    def test_exists_without_file(self):
        """Test exists check when file doesn't exist."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            cache_file = f.name

        try:
            cache = DatasetCache(cache_file=cache_file)
            cache.set("/nonexistent/file.csv", "85313NED", 100)

            # Should return False and remove stale entry
            assert cache.exists("/nonexistent/file.csv") == False
            assert cache.get("/nonexistent/file.csv") is None
        finally:
            if os.path.exists(cache_file):
                os.remove(cache_file)

    def test_remove_entry(self):
        """Test removing cache entry."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            cache_file = f.name

        try:
            cache = DatasetCache(cache_file=cache_file)
            cache.set("/path/to/file.csv", "85313NED", 100)
            cache.remove("/path/to/file.csv")

            assert cache.get("/path/to/file.csv") is None
        finally:
            if os.path.exists(cache_file):
                os.remove(cache_file)
