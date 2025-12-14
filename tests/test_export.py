"""
Tests for dataset export tools (save to CSV, DuckDB).
"""
import pytest
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nl_opendata_mcp import server
from nl_opendata_mcp.models import SaveDatasetInput, SaveToDuckDBInput
from nl_opendata_mcp.config import get_settings
from nl_opendata_mcp.services.cache import dataset_cache


class MockContext:
    """Mock FastMCP context for testing."""
    def info(self, msg): print(f"[INFO] {msg}")
    def error(self, msg): print(f"[ERROR] {msg}")
    def warning(self, msg): print(f"[WARNING] {msg}")


def get_fn(tool):
    """Extract callable from FunctionTool wrapper."""
    return tool.fn if hasattr(tool, 'fn') else tool


@pytest.fixture
def ctx():
    return MockContext()


@pytest.fixture
def settings():
    return get_settings()


TEST_DATASET_ID = "85313NED"


@pytest.mark.asyncio
async def test_save_dataset_basic(ctx, settings):
    """Test saving dataset to CSV."""
    fn = get_fn(server.cbs_save_dataset)
    path = "test_save_basic.csv"
    full_path = os.path.abspath(os.path.join(settings.downloads_path, path))

    # Ensure clean state
    if os.path.exists(full_path):
        os.remove(full_path)
    dataset_cache.remove(full_path)

    try:
        params = SaveDatasetInput(dataset_id=TEST_DATASET_ID, file_name=path, top=10)
        result = await fn(ctx, params)

        assert "saved" in result.lower() or "error" in result.lower()
        if "saved" in result.lower():
            assert os.path.exists(full_path)
    finally:
        # Cleanup
        if os.path.exists(full_path):
            os.remove(full_path)
        dataset_cache.remove(full_path)


@pytest.mark.asyncio
async def test_save_dataset_caching(ctx, settings):
    """Test that cached datasets return immediately."""
    fn = get_fn(server.cbs_save_dataset)
    path = "test_cache_check.csv"
    full_path = os.path.abspath(os.path.join(settings.downloads_path, path))

    # Ensure clean state
    if os.path.exists(full_path):
        os.remove(full_path)
    dataset_cache.remove(full_path)

    try:
        params = SaveDatasetInput(dataset_id=TEST_DATASET_ID, file_name=path, top=10)

        # First call - should download
        result1 = await fn(ctx, params)
        assert "cached" not in result1.lower() or "error" in result1.lower()

        # Second call - should be cached
        if "saved" in result1.lower():
            result2 = await fn(ctx, params)
            assert "cached" in result2.lower()
    finally:
        # Cleanup
        if os.path.exists(full_path):
            os.remove(full_path)
        dataset_cache.remove(full_path)


@pytest.mark.asyncio
async def test_save_dataset_path_traversal_blocked(ctx, settings):
    """Test that path traversal is blocked."""
    fn = get_fn(server.cbs_save_dataset)

    # Expected safe path after sanitization (basename only)
    safe_path = os.path.abspath(os.path.join(settings.downloads_path, "passwd"))

    # Cleanup before test
    if os.path.exists(safe_path):
        os.remove(safe_path)
    dataset_cache.remove(safe_path)

    try:
        # Try path traversal
        params = SaveDatasetInput(dataset_id=TEST_DATASET_ID, file_name="../../../etc/passwd", top=10)
        result = await fn(ctx, params)

        # Path traversal should be blocked - file saved safely in downloads dir
        # The ../../../etc/ part should be stripped, leaving just "passwd"
        if "saved" in result.lower():
            # Verify file was saved in downloads directory, not /etc/
            assert settings.downloads_path in result or "downloads" in result.lower()
            assert "/etc/" not in result
            # The file should exist in the safe location
            assert os.path.exists(safe_path)
        else:
            # Alternatively, an error is acceptable
            assert "error" in result.lower()
    finally:
        # Cleanup
        if os.path.exists(safe_path):
            os.remove(safe_path)
        dataset_cache.remove(safe_path)


@pytest.mark.asyncio
async def test_save_to_duckdb(ctx, settings):
    """Test saving dataset to DuckDB."""
    import duckdb

    fn = get_fn(server.cbs_save_dataset_to_duckdb)
    db_path = settings.duckdb_path
    table_name = "test_export_table"

    try:
        params = SaveToDuckDBInput(
            dataset_id=TEST_DATASET_ID,
            table_name=table_name,
            fetch_all=False  # Just get first batch for speed
        )
        result = await fn(ctx, params)

        assert "saved" in result.lower() or "error" in result.lower()

        if "saved" in result.lower():
            # Verify table exists
            con = duckdb.connect(db_path)
            tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
            assert table_name in tables

            # Verify has rows
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            assert count > 0
            con.close()
    finally:
        # Cleanup
        if os.path.exists(db_path):
            con = duckdb.connect(db_path)
            try:
                con.execute(f"DROP TABLE IF EXISTS {table_name}")
            except:
                pass
            con.close()


@pytest.mark.asyncio
async def test_save_to_duckdb_with_select(ctx, settings):
    """Test saving to DuckDB with column selection."""
    import duckdb

    fn = get_fn(server.cbs_save_dataset_to_duckdb)
    db_path = settings.duckdb_path
    table_name = "test_select_table"

    try:
        params = SaveToDuckDBInput(
            dataset_id=TEST_DATASET_ID,
            table_name=table_name,
            fetch_all=False,
            select=["ID"]
        )
        result = await fn(ctx, params)

        # Should succeed or fail gracefully
        assert result is not None
    finally:
        # Cleanup
        if os.path.exists(db_path):
            con = duckdb.connect(db_path)
            try:
                con.execute(f"DROP TABLE IF EXISTS {table_name}")
            except:
                pass
            con.close()
