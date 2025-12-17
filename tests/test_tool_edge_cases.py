"""
Tests for tool edge cases and error handling.
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class MockContext:
    """Mock FastMCP context for testing."""

    def __init__(self):
        self.messages = []

    def info(self, msg):
        self.messages.append(('info', msg))

    def error(self, msg):
        self.messages.append(('error', msg))

    def warning(self, msg):
        self.messages.append(('warning', msg))


class TestSearchEdgeCases:
    @pytest.mark.asyncio
    async def test_search_with_special_characters(self):
        """Search should handle quotes and special chars in query."""
        from nl_opendata_mcp.tools.discovery import cbs_search_datasets
        from nl_opendata_mcp.models import SearchDatasetsInput

        ctx = MockContext()
        result = await cbs_search_datasets(
            ctx,
            SearchDatasetsInput(query="test's \"value\"", top=5)
        )

        # Should not crash, should return something
        assert result is not None

    @pytest.mark.asyncio
    async def test_search_with_unicode(self):
        """Search should handle unicode characters."""
        from nl_opendata_mcp.tools.discovery import cbs_search_datasets
        from nl_opendata_mcp.models import SearchDatasetsInput

        ctx = MockContext()
        result = await cbs_search_datasets(
            ctx,
            SearchDatasetsInput(query="café München", top=5)
        )

        assert result is not None

    @pytest.mark.asyncio
    async def test_search_very_long_query(self):
        """Search should handle or reject very long queries gracefully."""
        from nl_opendata_mcp.tools.discovery import cbs_search_datasets
        from nl_opendata_mcp.models import SearchDatasetsInput

        ctx = MockContext()
        long_query = "bevolking " * 100  # Very long query

        result = await cbs_search_datasets(
            ctx,
            SearchDatasetsInput(query=long_query, top=5)
        )

        # Should handle gracefully (either work or return error message)
        assert result is not None


class TestQueryEdgeCases:
    @pytest.mark.asyncio
    async def test_query_nonexistent_dataset(self):
        """Should return helpful error for invalid dataset."""
        from nl_opendata_mcp.tools.query import cbs_query_dataset
        from nl_opendata_mcp.models import QueryDatasetInput

        ctx = MockContext()
        result = await cbs_query_dataset(
            ctx,
            QueryDatasetInput(dataset_id="NONEXISTENT99999XYZ", top=5)
        )

        # Should indicate error/not found
        result_lower = result.lower()
        assert "not found" in result_lower or "error" in result_lower or "failed" in result_lower

    @pytest.mark.asyncio
    async def test_query_returns_data(self):
        """Query results should contain data."""
        from nl_opendata_mcp.tools.query import cbs_query_dataset
        from nl_opendata_mcp.models import QueryDatasetInput

        ctx = MockContext()
        result = await cbs_query_dataset(
            ctx,
            QueryDatasetInput(dataset_id="83765NED", top=3)
        )

        # Should return some data (contains comma-separated values)
        assert result is not None
        assert len(result) > 50  # Should have meaningful content
        # Should have multiple lines (header + data)
        assert result.count('\n') >= 1

    @pytest.mark.asyncio
    async def test_query_with_filter(self):
        """Query with filter should work or return empty result."""
        from nl_opendata_mcp.tools.query import cbs_query_dataset
        from nl_opendata_mcp.models import QueryDatasetInput

        ctx = MockContext()
        # Use a filter that might match something in this geo dataset
        result = await cbs_query_dataset(
            ctx,
            QueryDatasetInput(
                dataset_id="83765NED",
                filter="SoortRegio_2 eq 'Gemeente'",
                top=5
            )
        )

        # Should not crash
        assert result is not None

    @pytest.mark.asyncio
    async def test_query_respects_top_limit(self):
        """Query should respect the top parameter."""
        from nl_opendata_mcp.tools.query import cbs_query_dataset
        from nl_opendata_mcp.models import QueryDatasetInput

        ctx = MockContext()
        result = await cbs_query_dataset(
            ctx,
            QueryDatasetInput(dataset_id="83765NED", top=2)
        )

        # Result includes metadata header like "QUERY RESULT: ...\nRows: 2, ..."
        # Check that "Rows: 2" is mentioned in the output
        assert "Rows: 2" in result or "rows: 2" in result.lower()


class TestInspectEdgeCases:
    @pytest.mark.asyncio
    async def test_inspect_includes_key_sections(self):
        """Inspect should return metadata and structure info."""
        from nl_opendata_mcp.tools.query import cbs_inspect_dataset_details
        from nl_opendata_mcp.models import DatasetIdInput

        ctx = MockContext()
        result = await cbs_inspect_dataset_details(
            ctx,
            DatasetIdInput(dataset_id="83765NED")
        )

        result_upper = result.upper()
        # Should have some structural info (dimensions, measures, or title)
        assert "DIMENSIONS" in result_upper or "MEASURES" in result_upper or "TITLE" in result_upper

    @pytest.mark.asyncio
    async def test_inspect_nonexistent_dataset(self):
        """Inspect of invalid dataset should return error message."""
        from nl_opendata_mcp.tools.query import cbs_inspect_dataset_details
        from nl_opendata_mcp.models import DatasetIdInput

        ctx = MockContext()
        result = await cbs_inspect_dataset_details(
            ctx,
            DatasetIdInput(dataset_id="INVALID_DATASET_XYZ123")
        )

        result_lower = result.lower()
        assert "not found" in result_lower or "error" in result_lower


class TestMetadataEdgeCases:
    @pytest.mark.asyncio
    async def test_get_metadata_for_valid_dataset(self):
        """Should return metadata for valid dataset using unified tool."""
        from nl_opendata_mcp.tools.metadata import cbs_get_metadata
        from nl_opendata_mcp.models import GetMetadataInput, MetadataType

        ctx = MockContext()
        result = await cbs_get_metadata(
            ctx,
            GetMetadataInput(dataset_id="83765NED", metadata_type=MetadataType.INFO)
        )

        # Should contain dataset info
        assert "83765NED" in result or "Title" in result or "title" in result.lower()

    @pytest.mark.asyncio
    async def test_query_metadata_endpoint(self):
        """Should query specific metadata endpoints using unified tool."""
        from nl_opendata_mcp.tools.metadata import cbs_get_metadata
        from nl_opendata_mcp.models import GetMetadataInput, MetadataType

        ctx = MockContext()
        # Use WijkenEnBuurten which exists in 83765NED via custom endpoint
        result = await cbs_get_metadata(
            ctx,
            GetMetadataInput(dataset_id="83765NED", metadata_type=MetadataType.CUSTOM, endpoint_name="WijkenEnBuurten")
        )

        # Should return location data (Key and Title columns)
        assert "Key" in result or "Title" in result or "GM" in result

    @pytest.mark.asyncio
    async def test_query_nonexistent_metadata_endpoint(self):
        """Should handle nonexistent metadata endpoint gracefully."""
        from nl_opendata_mcp.tools.metadata import cbs_get_metadata
        from nl_opendata_mcp.models import GetMetadataInput, MetadataType

        ctx = MockContext()
        result = await cbs_get_metadata(
            ctx,
            GetMetadataInput(dataset_id="83765NED", metadata_type=MetadataType.CUSTOM, endpoint_name="NonExistentEndpoint")
        )

        # Should return error message, not crash
        result_lower = result.lower()
        assert "error" in result_lower or "not found" in result_lower


class TestEstimateSizeEdgeCases:
    @pytest.mark.asyncio
    async def test_estimate_returns_numeric_info(self):
        """Size estimate should include row/column counts."""
        from nl_opendata_mcp.tools.query import cbs_estimate_dataset_size
        from nl_opendata_mcp.models import DatasetIdInput

        ctx = MockContext()
        result = await cbs_estimate_dataset_size(
            ctx,
            DatasetIdInput(dataset_id="83765NED")
        )

        # Should mention rows or records
        result_lower = result.lower()
        assert "row" in result_lower or "record" in result_lower or "estimated" in result_lower


class TestAvailabilityEdgeCases:
    @pytest.mark.asyncio
    async def test_check_cbs_dataset(self):
        """Should confirm CBS dataset is available."""
        from nl_opendata_mcp.tools.discovery import cbs_check_dataset_availability
        from nl_opendata_mcp.models import DatasetIdInput

        ctx = MockContext()
        result = await cbs_check_dataset_availability(
            ctx,
            DatasetIdInput(dataset_id="83765NED")
        )

        result_lower = result.lower()
        assert "cbs" in result_lower or "available" in result_lower or "odata" in result_lower

    @pytest.mark.asyncio
    async def test_check_invalid_dataset(self):
        """Should indicate dataset not found."""
        from nl_opendata_mcp.tools.discovery import cbs_check_dataset_availability
        from nl_opendata_mcp.models import DatasetIdInput

        ctx = MockContext()
        result = await cbs_check_dataset_availability(
            ctx,
            DatasetIdInput(dataset_id="TOTALLY_FAKE_12345")
        )

        result_lower = result.lower()
        assert "not found" in result_lower or "not" in result_lower
