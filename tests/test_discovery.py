"""
Tests for dataset discovery tools (list, search, check availability).
"""
import pytest
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nl_opendata_mcp import server
from nl_opendata_mcp.models import (
    ListDatasetsInput,
    SearchDatasetsInput,
    SearchField,
    DatasetIdInput,
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


@pytest.mark.asyncio
async def test_list_datasets(ctx):
    """Test listing datasets from catalog."""
    fn = get_fn(server.cbs_list_datasets)
    params = ListDatasetsInput(top=5)
    result = await fn(ctx, params)

    assert result is not None
    assert len(result) > 0
    assert "Identifier" in result
    assert "Title" in result


@pytest.mark.asyncio
async def test_list_datasets_pagination(ctx):
    """Test pagination in list datasets."""
    fn = get_fn(server.cbs_list_datasets)

    # Get first page
    params1 = ListDatasetsInput(top=3, skip=0)
    result1 = await fn(ctx, params1)

    # Get second page
    params2 = ListDatasetsInput(top=3, skip=3)
    result2 = await fn(ctx, params2)

    # Results should be different
    assert result1 != result2


@pytest.mark.asyncio
async def test_search_datasets(ctx):
    """Test searching datasets by keyword."""
    fn = get_fn(server.cbs_search_datasets)
    params = SearchDatasetsInput(query="Bevolking", top=5)
    result = await fn(ctx, params)

    assert result is not None
    assert "Identifier" in result or "No matching" in result


@pytest.mark.asyncio
async def test_search_datasets_title_only(ctx):
    """Test searching only in titles."""
    fn = get_fn(server.cbs_search_datasets)
    params = SearchDatasetsInput(query="Bevolking", top=5, search_field=SearchField.TITLE)
    result = await fn(ctx, params)

    assert result is not None


@pytest.mark.asyncio
async def test_search_datasets_summary_only(ctx):
    """Test searching only in summaries."""
    fn = get_fn(server.cbs_search_datasets)
    params = SearchDatasetsInput(query="statistiek", top=5, search_field=SearchField.SUMMARY)
    result = await fn(ctx, params)

    assert result is not None


@pytest.mark.asyncio
async def test_check_dataset_availability_cbs(ctx):
    """Test checking availability for CBS dataset."""
    fn = get_fn(server.cbs_check_dataset_availability)
    params = DatasetIdInput(dataset_id="85313NED")
    result = await fn(ctx, params)

    assert "85313NED" in result
    assert "CBS OData" in result or "not found" in result


@pytest.mark.asyncio
async def test_check_dataset_availability_invalid(ctx):
    """Test checking availability for non-existent dataset."""
    fn = get_fn(server.cbs_check_dataset_availability)
    params = DatasetIdInput(dataset_id="NONEXISTENT999")
    result = await fn(ctx, params)

    assert "not found" in result.lower()
