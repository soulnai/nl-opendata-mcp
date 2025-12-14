"""
Tests for dataset metadata tools.
"""
import pytest
import json
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nl_opendata_mcp import server
from nl_opendata_mcp.models import (
    DatasetIdInput,
    QueryMetadataInput,
    GetMetadataInput,
    MetadataType,
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
async def test_get_dataset_info(ctx):
    """Test getting dataset info (TableInfos)."""
    fn = get_fn(server.cbs_get_dataset_info)
    params = DatasetIdInput(dataset_id=TEST_DATASET_ID)
    result = await fn(ctx, params)

    assert result is not None
    assert len(result) > 0
    # Should be CSV format
    assert "," in result or "No info" in result


@pytest.mark.asyncio
async def test_get_table_structure(ctx):
    """Test getting table structure (DataProperties)."""
    fn = get_fn(server.cbs_get_table_structure)
    params = DatasetIdInput(dataset_id=TEST_DATASET_ID)
    result = await fn(ctx, params)

    assert result is not None
    # Should contain column info
    assert "Key" in result or "Type" in result or "No table" in result


@pytest.mark.asyncio
async def test_get_dataset_metadata(ctx):
    """Test getting dataset metadata endpoints."""
    fn = get_fn(server.cbs_get_dataset_metadata)
    params = DatasetIdInput(dataset_id=TEST_DATASET_ID)
    result = await fn(ctx, params)

    assert result is not None
    # Should be valid JSON
    try:
        data = json.loads(result)
        assert "odata.metadata" in data or "error" in result.lower()
    except json.JSONDecodeError:
        assert "error" in result.lower()


@pytest.mark.asyncio
async def test_query_dataset_metadata(ctx):
    """Test querying specific metadata endpoint."""
    fn = get_fn(server.cbs_query_dataset_metadata)
    params = QueryMetadataInput(dataset_id=TEST_DATASET_ID, metadata_name="DataProperties")
    result = await fn(ctx, params)

    assert result is not None
    # Should be valid JSON
    try:
        data = json.loads(result)
        assert isinstance(data, dict)
    except json.JSONDecodeError:
        assert "error" in result.lower()


@pytest.mark.asyncio
async def test_unified_metadata_info(ctx):
    """Test unified metadata tool - info type."""
    fn = get_fn(server.cbs_get_metadata)
    params = GetMetadataInput(dataset_id=TEST_DATASET_ID, metadata_type=MetadataType.INFO)
    result = await fn(ctx, params)

    assert result is not None
    assert len(result) > 0


@pytest.mark.asyncio
async def test_unified_metadata_structure(ctx):
    """Test unified metadata tool - structure type."""
    fn = get_fn(server.cbs_get_metadata)
    params = GetMetadataInput(dataset_id=TEST_DATASET_ID, metadata_type=MetadataType.STRUCTURE)
    result = await fn(ctx, params)

    assert result is not None
    # Should be CSV format with column info
    assert "Key" in result or "Type" in result or "No metadata" in result


@pytest.mark.asyncio
async def test_unified_metadata_endpoints(ctx):
    """Test unified metadata tool - endpoints type."""
    fn = get_fn(server.cbs_get_metadata)
    params = GetMetadataInput(dataset_id=TEST_DATASET_ID, metadata_type=MetadataType.ENDPOINTS)
    result = await fn(ctx, params)

    assert result is not None
    # Should be JSON
    try:
        data = json.loads(result)
        assert isinstance(data, dict)
    except json.JSONDecodeError:
        pytest.fail("Expected JSON output for endpoints")


@pytest.mark.asyncio
async def test_unified_metadata_custom(ctx):
    """Test unified metadata tool - custom endpoint."""
    fn = get_fn(server.cbs_get_metadata)
    params = GetMetadataInput(
        dataset_id=TEST_DATASET_ID,
        metadata_type=MetadataType.CUSTOM,
        custom_endpoint="DataProperties"
    )
    result = await fn(ctx, params)

    assert result is not None


@pytest.mark.asyncio
async def test_unified_metadata_custom_missing_endpoint(ctx):
    """Test unified metadata tool - custom without endpoint."""
    fn = get_fn(server.cbs_get_metadata)
    params = GetMetadataInput(
        dataset_id=TEST_DATASET_ID,
        metadata_type=MetadataType.CUSTOM
    )
    result = await fn(ctx, params)

    assert "error" in result.lower()
    assert "custom_endpoint" in result.lower()
