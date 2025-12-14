"""
CBS dimension value translator for nl-opendata-mcp server.

This module provides automatic translation of coded CBS dimension values
to human-readable text using dimension metadata endpoints.

Example coded values:
    - Geslacht: "3000   " -> "Mannen"
    - Perioden: "2023JJ00" -> "2023"
    - RegioS: "GM0363  " -> "Amsterdam"

Features:
    - Caching of dimension mappings (1 hour TTL)
    - Automatic detection of dimension columns
    - Batch translation of DataFrames

Example:
    >>> from nl_opendata_mcp.services import DimensionTranslator
    >>>
    >>> translator = DimensionTranslator()
    >>> df = await translator.translate_dataframe(df, "84826NED")
"""
import asyncio
import logging
import time
from typing import Optional
from io import StringIO

import pandas as pd

from .http_client import fetch_json

logger = logging.getLogger(__name__)

# CBS OData API base URL
CBS_ODATA_BASE = "https://opendata.cbs.nl/ODataApi/OData"


class DimensionCache:
    """
    Cache for CBS dimension metadata (Key -> Title mappings).

    Stores dimension value mappings with TTL-based expiration.
    """

    def __init__(self, ttl_seconds: int = 3600):
        """
        Initialize dimension cache.

        Args:
            ttl_seconds: Time-to-live for cache entries (default: 1 hour)
        """
        self._cache: dict[str, dict[str, str]] = {}
        self._timestamps: dict[str, float] = {}
        self._ttl = ttl_seconds
        self._lock = asyncio.Lock()

    def _is_valid(self, cache_key: str) -> bool:
        """Check if cache entry is valid (exists and not expired)."""
        if cache_key not in self._cache:
            return False
        if cache_key not in self._timestamps:
            return False
        age = time.time() - self._timestamps[cache_key]
        return age < self._ttl

    async def get_mapping(
        self,
        dataset_id: str,
        dimension_name: str
    ) -> dict[str, str]:
        """
        Get Key -> Title mapping for a dimension.

        Args:
            dataset_id: CBS dataset identifier (e.g., "84826NED")
            dimension_name: Dimension name (e.g., "Geslacht", "Perioden")

        Returns:
            Dictionary mapping dimension keys to titles
        """
        cache_key = f"{dataset_id}:{dimension_name}"

        if self._is_valid(cache_key):
            return self._cache[cache_key]

        async with self._lock:
            # Double-check after acquiring lock
            if self._is_valid(cache_key):
                return self._cache[cache_key]

            # Fetch from API
            mapping = await self._fetch_dimension(dataset_id, dimension_name)
            self._cache[cache_key] = mapping
            self._timestamps[cache_key] = time.time()
            logger.debug(f"Cached {len(mapping)} values for {cache_key}")
            return mapping

    async def _fetch_dimension(
        self,
        dataset_id: str,
        dimension_name: str
    ) -> dict[str, str]:
        """
        Fetch dimension metadata from CBS API.

        Args:
            dataset_id: CBS dataset identifier
            dimension_name: Dimension name

        Returns:
            Dictionary mapping dimension keys to titles
        """
        url = f"{CBS_ODATA_BASE}/{dataset_id}/{dimension_name}"

        try:
            data = await fetch_json(url, default={"value": []})

            mapping = {}
            for item in data.get("value", []):
                key = item.get("Key", "")
                title = item.get("Title", "")
                if key and title:
                    # Store both stripped and original key for flexible matching
                    mapping[key.strip()] = title
                    if key != key.strip():
                        mapping[key] = title

            return mapping

        except Exception as e:
            logger.warning(f"Failed to fetch dimension {dimension_name} for {dataset_id}: {e}")
            return {}

    def clear(self):
        """Clear all cached mappings."""
        self._cache.clear()
        self._timestamps.clear()
        logger.info("Dimension cache cleared")

    def get_stats(self) -> dict:
        """Get cache statistics."""
        valid_count = sum(1 for k in self._cache if self._is_valid(k))
        return {
            "total_entries": len(self._cache),
            "valid_entries": valid_count,
            "ttl_seconds": self._ttl
        }


class DimensionTranslator:
    """
    Translates coded CBS dimension values to human-readable text.

    Automatically detects dimension columns and translates values
    using cached dimension metadata.
    """

    def __init__(self, cache: Optional[DimensionCache] = None):
        """
        Initialize translator.

        Args:
            cache: Optional DimensionCache instance (creates new if None)
        """
        self._cache = cache or dimension_cache

    async def get_dimension_columns(self, dataset_id: str) -> dict[str, str]:
        """
        Get dimension columns from DataProperties.

        Args:
            dataset_id: CBS dataset identifier

        Returns:
            Dictionary mapping column key to dimension type
            (e.g., {"Geslacht": "Dimension", "Perioden": "TimeDimension"})
        """
        url = f"{CBS_ODATA_BASE}/{dataset_id}/DataProperties"

        try:
            data = await fetch_json(url, default={"value": []})

            dimensions = {}
            for prop in data.get("value", []):
                prop_type = prop.get("Type", "")
                prop_key = prop.get("Key", "")

                # Dimension types: Dimension, TimeDimension, GeoDimension
                if "Dimension" in prop_type and prop_key:
                    dimensions[prop_key] = prop_type

            return dimensions

        except Exception as e:
            logger.warning(f"Failed to get dimension columns for {dataset_id}: {e}")
            return {}

    async def get_column_titles(self, dataset_id: str) -> dict[str, str]:
        """
        Get human-readable titles for all columns from DataProperties.

        Args:
            dataset_id: CBS dataset identifier

        Returns:
            Dictionary mapping column Key to Title
            (e.g., {"ProvincialeHeffingenInMlnEuro_1": "Provinciale heffingen in mln euro"})
        """
        url = f"{CBS_ODATA_BASE}/{dataset_id}/DataProperties"

        try:
            data = await fetch_json(url, default={"value": []})

            titles = {}
            for prop in data.get("value", []):
                key = prop.get("Key", "")
                title = prop.get("Title", "")
                if key and title and key != title:
                    titles[key] = title

            return titles

        except Exception as e:
            logger.warning(f"Failed to get column titles for {dataset_id}: {e}")
            return {}

    async def get_available_dimensions(self, dataset_id: str) -> list[str]:
        """
        Get list of available dimension metadata endpoints for a dataset.

        Args:
            dataset_id: CBS dataset identifier

        Returns:
            List of dimension endpoint names
        """
        url = f"{CBS_ODATA_BASE}/{dataset_id}"

        try:
            data = await fetch_json(url, default={"value": []})

            # Standard non-dimension endpoints to exclude
            standard_endpoints = {
                "TableInfos", "UntypedDataSet", "TypedDataSet",
                "DataProperties", "CategoryGroups"
            }

            dimensions = []
            for item in data.get("value", []):
                name = item.get("name", "")
                if name and name not in standard_endpoints:
                    dimensions.append(name)

            return dimensions

        except Exception as e:
            logger.warning(f"Failed to get available dimensions for {dataset_id}: {e}")
            return []

    async def translate_value(
        self,
        dataset_id: str,
        dimension_name: str,
        value: str
    ) -> str:
        """
        Translate a single dimension value.

        Args:
            dataset_id: CBS dataset identifier
            dimension_name: Dimension name
            value: Coded value to translate

        Returns:
            Translated title or original value if not found
        """
        if not value or pd.isna(value):
            return value

        mapping = await self._cache.get_mapping(dataset_id, dimension_name)

        # Try both stripped and original value
        str_value = str(value)
        if str_value.strip() in mapping:
            return mapping[str_value.strip()]
        if str_value in mapping:
            return mapping[str_value]

        return value

    async def translate_dataframe(
        self,
        df: pd.DataFrame,
        dataset_id: str,
        dimension_columns: Optional[list[str]] = None,
        translate_column_names: bool = False,
        skip_columns: Optional[list[str]] = None
    ) -> pd.DataFrame:
        """
        Translate all dimension columns in a DataFrame.

        Args:
            df: DataFrame with coded dimension values
            dataset_id: CBS dataset identifier
            dimension_columns: Specific columns to translate (auto-detects if None)
            translate_column_names: Also translate column headers to human-readable titles (default: False to keep valid Python identifiers)
            skip_columns: Columns to skip translation (default: ['Perioden'] to preserve filterable codes)

        Returns:
            DataFrame with translated dimension values and optionally translated column names
        """
        if df.empty:
            return df

        # Default: skip Perioden to preserve original codes for filtering
        if skip_columns is None:
            skip_columns = ['Perioden']

        # Auto-detect dimension columns if not specified
        if dimension_columns is None:
            available_dims = await self.get_available_dimensions(dataset_id)
            # Only translate columns that exist in both DataFrame and available dimensions
            # Exclude skip_columns from translation
            dimension_columns = [
                col for col in df.columns
                if col in available_dims and col not in skip_columns
            ]

        translated_df = df.copy()

        # Translate dimension values if there are dimension columns
        if dimension_columns:
            # Fetch all dimension mappings in parallel
            mappings = {}
            tasks = []
            for col in dimension_columns:
                if col in translated_df.columns:
                    tasks.append(self._cache.get_mapping(dataset_id, col))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for col, result in zip(dimension_columns, results):
                    if isinstance(result, dict):
                        mappings[col] = result
                    else:
                        logger.warning(f"Failed to get mapping for {col}: {result}")

            # Apply value translations
            for col, mapping in mappings.items():
                if col in translated_df.columns and mapping:
                    translated_df[col] = translated_df[col].apply(
                        lambda x: mapping.get(str(x).strip(), mapping.get(str(x), x))
                        if pd.notna(x) else x
                    )

        # Translate column names to human-readable titles
        if translate_column_names:
            column_titles = await self.get_column_titles(dataset_id)
            if column_titles:
                # Build rename mapping for columns that have titles
                rename_map = {
                    col: column_titles[col]
                    for col in translated_df.columns
                    if col in column_titles
                }
                if rename_map:
                    translated_df = translated_df.rename(columns=rename_map)
                    logger.debug(f"Renamed {len(rename_map)} columns for {dataset_id}")

        return translated_df

    async def translate_csv(
        self,
        csv_data: str,
        dataset_id: str,
        dimension_columns: Optional[list[str]] = None
    ) -> str:
        """
        Translate dimension values in CSV data.

        Args:
            csv_data: CSV string with coded dimension values
            dataset_id: CBS dataset identifier
            dimension_columns: Specific columns to translate (auto-detects if None)

        Returns:
            CSV string with translated dimension values
        """
        try:
            df = pd.read_csv(StringIO(csv_data))
            translated_df = await self.translate_dataframe(df, dataset_id, dimension_columns)
            return translated_df.to_csv(index=False)
        except Exception as e:
            logger.warning(f"Failed to translate CSV: {e}")
            return csv_data


# Global instances
dimension_cache = DimensionCache(ttl_seconds=3600)
translator = DimensionTranslator(dimension_cache)
