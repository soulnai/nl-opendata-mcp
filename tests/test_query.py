"""
Tests for dataset query and inspection tools.
"""
import pytest
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nl_opendata_mcp import server
from nl_opendata_mcp.models import (
    DatasetIdInput,
    QueryDatasetInput,
)


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


TEST_DATASET_ID = "85313NED"


@pytest.mark.asyncio
async def test_estimate_dataset_size(ctx):
    """Test estimating dataset size."""
    fn = get_fn(server.cbs_estimate_dataset_size)
    params = DatasetIdInput(dataset_id=TEST_DATASET_ID)
    result = await fn(ctx, params)

    assert result is not None
    assert "DATASET SIZE ESTIMATE" in result
    assert "Columns:" in result
    assert "Estimated rows:" in result


@pytest.mark.asyncio
async def test_query_dataset_basic(ctx):
    """Test basic dataset query."""
    fn = get_fn(server.cbs_query_dataset)
    params = QueryDatasetInput(dataset_id=TEST_DATASET_ID, top=5)
    result = await fn(ctx, params)

    assert result is not None
    assert len(result) > 0


@pytest.mark.asyncio
async def test_query_dataset_with_skip(ctx):
    """Test dataset query with pagination."""
    fn = get_fn(server.cbs_query_dataset)

    # First query
    params1 = QueryDatasetInput(dataset_id=TEST_DATASET_ID, top=3, skip=0)
    result1 = await fn(ctx, params1)

    # Second query with skip
    params2 = QueryDatasetInput(dataset_id=TEST_DATASET_ID, top=3, skip=3)
    result2 = await fn(ctx, params2)

    # Results should be different (unless dataset is very small)
    assert result1 is not None
    assert result2 is not None


@pytest.mark.asyncio
async def test_query_dataset_compact_mode(ctx):
    """Test compact mode for large results."""
    fn = get_fn(server.cbs_query_dataset)
    params = QueryDatasetInput(dataset_id=TEST_DATASET_ID, top=200, compact=True)
    result = await fn(ctx, params)

    # With compact=True and many rows, should return summary
    assert result is not None
    # Either full CSV or compact summary
    assert "," in result or "QUERY RESULT" in result


@pytest.mark.asyncio
async def test_query_dataset_with_select(ctx):
    """Test query with column selection."""
    fn = get_fn(server.cbs_query_dataset)
    params = QueryDatasetInput(
        dataset_id=TEST_DATASET_ID,
        top=5,
        select=["ID"]  # Most datasets have an ID field
    )
    result = await fn(ctx, params)

    assert result is not None


@pytest.mark.asyncio
async def test_inspect_dataset_details(ctx):
    """Test comprehensive dataset inspection."""
    fn = get_fn(server.cbs_inspect_dataset_details)
    params = DatasetIdInput(dataset_id=TEST_DATASET_ID)
    result = await fn(ctx, params)

    assert result is not None
    assert "DATASET:" in result
    assert TEST_DATASET_ID in result
    # Should include dimensions or measures info
    assert "DIMENSIONS" in result or "MEASURES" in result or "Title:" in result


@pytest.mark.asyncio
async def test_inspect_nonexistent_dataset(ctx):
    """Test inspection of non-existent dataset."""
    fn = get_fn(server.cbs_inspect_dataset_details)
    params = DatasetIdInput(dataset_id="NONEXISTENT999")
    result = await fn(ctx, params)

    assert result is not None
    assert "not found" in result.lower() or "error" in result.lower()
