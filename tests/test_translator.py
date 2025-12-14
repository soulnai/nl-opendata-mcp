"""
Tests for dimension translator service.
"""
from nl_opendata_mcp import server
import pytest
import pandas as pd
from unittest.mock import patch, AsyncMock

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nl_opendata_mcp.services.translator import (
    DimensionCache,
    DimensionTranslator,
)


class TestDimensionCache:
    @pytest.mark.asyncio
    async def test_caches_mapping_on_second_call(self):
        """Cache should return stored mapping without re-fetching."""
        cache = DimensionCache(ttl_seconds=3600)

        with patch('nl_opendata_mcp.services.translator.fetch_json', new_callable=AsyncMock) as mock:
            mock.return_value = {
                "value": [
                    {"Key": "1100", "Title": "Mannen"},
                    {"Key": "1200", "Title": "Vrouwen"},
                ]
            }

            await cache.get_mapping("83765NED", "Geslacht")
            await cache.get_mapping("83765NED", "Geslacht")

            assert mock.call_count == 1  # Only fetched once

    @pytest.mark.asyncio
    async def test_strips_whitespace_from_keys(self):
        """CBS keys often have trailing spaces - should handle both."""
        cache = DimensionCache(ttl_seconds=3600)

        with patch('nl_opendata_mcp.services.translator.fetch_json', new_callable=AsyncMock) as mock:
            mock.return_value = {
                "value": [{"Key": "1100   ", "Title": "Mannen"}]
            }

            mapping = await cache.get_mapping("test", "dim")

            # Should find with or without spaces
            assert mapping.get("1100") == "Mannen"
            assert mapping.get("1100   ") == "Mannen"

    @pytest.mark.asyncio
    async def test_handles_api_failure_gracefully(self):
        """Should return empty dict on API failure, not crash."""
        cache = DimensionCache(ttl_seconds=3600)

        with patch('nl_opendata_mcp.services.translator.fetch_json', new_callable=AsyncMock) as mock:
            mock.side_effect = Exception("API error")

            mapping = await cache.get_mapping("test", "dim")

            assert mapping == {}

    @pytest.mark.asyncio
    async def test_different_datasets_cached_separately(self):
        """Each dataset/dimension combo should have its own cache entry."""
        cache = DimensionCache(ttl_seconds=3600)

        with patch('nl_opendata_mcp.services.translator.fetch_json', new_callable=AsyncMock) as mock:
            mock.return_value = {"value": [{"Key": "1", "Title": "Test"}]}

            await cache.get_mapping("dataset1", "dim")
            await cache.get_mapping("dataset2", "dim")

            assert mock.call_count == 2  # Different datasets, both fetched

    def test_cache_stats(self):
        """Stats should report cache state."""
        cache = DimensionCache(ttl_seconds=3600)

        stats = cache.get_stats()

        assert "total_entries" in stats
        assert "ttl_seconds" in stats
        assert stats["ttl_seconds"] == 3600


class TestDimensionTranslator:
    @pytest.mark.asyncio
    async def test_translates_dimension_values(self):
        """Should replace coded values with titles."""
        translator = DimensionTranslator()

        df = pd.DataFrame({
            "Geslacht": ["1100", "1200"],
            "Value": [100, 200],
        })

        with patch.object(translator._cache, 'get_mapping', new_callable=AsyncMock) as cache_mock:
            cache_mock.return_value = {"1100": "Mannen", "1200": "Vrouwen"}

            with patch.object(translator, 'get_available_dimensions', new_callable=AsyncMock) as dims_mock:
                dims_mock.return_value = ["Geslacht"]

                result = await translator.translate_dataframe(df, "test")

                assert result["Geslacht"].tolist() == ["Mannen", "Vrouwen"]
                assert result["Value"].tolist() == [100, 200]  # Unchanged

    @pytest.mark.asyncio
    async def test_skips_perioden_by_default(self):
        """Perioden should not be translated (needed for filtering)."""
        translator = DimensionTranslator()

        df = pd.DataFrame({
            "Perioden": ["2023JJ00", "2024JJ00"],
        })

        with patch.object(translator, 'get_available_dimensions', new_callable=AsyncMock) as mock:
            mock.return_value = ["Perioden"]

            result = await translator.translate_dataframe(df, "test")

            assert result["Perioden"].tolist() == ["2023JJ00", "2024JJ00"]

    @pytest.mark.asyncio
    async def test_handles_missing_values(self):
        """Should preserve NaN/None values."""
        translator = DimensionTranslator()

        df = pd.DataFrame({
            "Geslacht": ["1100", None, "1200"],
        })

        with patch.object(translator._cache, 'get_mapping', new_callable=AsyncMock) as cache_mock:
            cache_mock.return_value = {"1100": "Mannen", "1200": "Vrouwen"}

            with patch.object(translator, 'get_available_dimensions', new_callable=AsyncMock) as dims_mock:
                dims_mock.return_value = ["Geslacht"]

                result = await translator.translate_dataframe(df, "test")

                assert result["Geslacht"].tolist()[0] == "Mannen"
                assert pd.isna(result["Geslacht"].tolist()[1])
                assert result["Geslacht"].tolist()[2] == "Vrouwen"

    @pytest.mark.asyncio
    async def test_handles_empty_dataframe(self):
        """Should handle empty DataFrame without error."""
        translator = DimensionTranslator()

        df = pd.DataFrame()

        result = await translator.translate_dataframe(df, "test")

        assert result.empty

    @pytest.mark.asyncio
    async def test_preserves_unknown_values(self):
        """Values not in mapping should remain unchanged."""
        translator = DimensionTranslator()

        df = pd.DataFrame({
            "Geslacht": ["1100", "9999"],  # 9999 not in mapping
        })

        with patch.object(translator._cache, 'get_mapping', new_callable=AsyncMock) as cache_mock:
            cache_mock.return_value = {"1100": "Mannen"}  # No mapping for 9999

            with patch.object(translator, 'get_available_dimensions', new_callable=AsyncMock) as dims_mock:
                dims_mock.return_value = ["Geslacht"]

                result = await translator.translate_dataframe(df, "test")

                assert result["Geslacht"].tolist()[0] == "Mannen"
                assert result["Geslacht"].tolist()[1] == "9999"  # Unchanged

    @pytest.mark.asyncio
    async def test_translate_specific_columns_only(self):
        """Should only translate specified columns when provided."""
        translator = DimensionTranslator()

        df = pd.DataFrame({
            "Geslacht": ["1100"],
            "RegioS": ["GM0363"],
        })

        with patch.object(translator._cache, 'get_mapping', new_callable=AsyncMock) as cache_mock:
            cache_mock.return_value = {"1100": "Mannen", "GM0363": "Amsterdam"}

            # Only translate Geslacht, not RegioS
            result = await translator.translate_dataframe(
                df, "test", dimension_columns=["Geslacht"]
            )

            assert result["Geslacht"].tolist() == ["Mannen"]
            assert result["RegioS"].tolist() == ["GM0363"]  # Not translated
