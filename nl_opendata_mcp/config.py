"""
Configuration management for nl-opendata-mcp server.

This module provides centralized configuration using pydantic-settings,
allowing configuration via environment variables or .env files.

Environment Variables:
    All settings can be overridden with NL_OPENDATA_MCP_ prefix:
    - NL_OPENDATA_MCP_CATALOG_BASE_URL: CBS catalog API URL
    - NL_OPENDATA_MCP_DATA_BASE_URL: CBS data API URL
    - NL_OPENDATA_MCP_HTTP_TIMEOUT: Request timeout in seconds
    - NL_OPENDATA_MCP_DOWNLOADS_PATH: Directory for downloaded files
    - NL_OPENDATA_MCP_TRANSPORT: Server transport (stdio/http/sse)
    - NL_OPENDATA_MCP_LOG_LEVEL: Logging level (DEBUG/INFO/WARNING/ERROR)
    - NL_OPENDATA_MCP_USE_PYTHON_ANALYSIS: Use Python analysis for datasets. Default: False

Example:
    >>> from nl_opendata_mcp.config import get_settings
    >>> settings = get_settings()
    >>> print(settings.data_base_url)
    'https://opendata.cbs.nl/ODataApi/OData'
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    """Server configuration with environment variable support."""

    # API Configuration
    catalog_base_url: str = Field(
        default="https://opendata.cbs.nl/ODataCatalog",
        description="CBS OData Catalog base URL"
    )
    data_base_url: str = Field(
        default="https://opendata.cbs.nl/ODataApi/OData",
        description="CBS OData API base URL"
    )
    ckan_base_url: str = Field(
        default="https://data.overheid.nl/data/api/3/action",
        description="data.overheid.nl CKAN API base URL"
    )

    # Timeouts
    http_timeout: float = Field(
        default=30.0,
        description="HTTP request timeout in seconds"
    )
    connect_timeout: float = Field(
        default=10.0,
        description="HTTP connection timeout in seconds"
    )

    # Paths
    downloads_path: str = Field(
        default="./downloads",
        description="Directory for downloaded datasets"
    )
    cache_file: str = Field(
        default="catalog_cache.json",
        description="Path to catalog cache file"
    )
    dataset_cache_file: str = Field(
        default="dataset_cache.json",
        description="Path to dataset cache file"
    )
    duckdb_path: str = Field(
        default="datasets.db",
        description="Path to DuckDB database file"
    )

    # Limits
    max_records_per_fetch: int = Field(
        default=1_000_000,
        description="Maximum records to fetch in a single operation"
    )
    batch_size: int = Field(
        default=1000,
        description="Batch size for paginated fetches"
    )
    duckdb_batch_size: int = Field(
        default=9000,
        description="Batch size for DuckDB imports"
    )

    # Server
    transport: str = Field(
        default="stdio",
        description="Transport mode: stdio, http, or sse"
    )
    host: str = Field(
        default="0.0.0.0",
        description="Server host for http/sse transport"
    )
    port: int = Field(
        default=8000,
        description="Server port for http/sse transport"
    )
    log_level: str = Field(
        default="ERROR",
        description="Logging level"
    )

    # HTTP Client
    max_connections: int = Field(
        default=100,
        description="Maximum HTTP connections"
    )
    max_keepalive_connections: int = Field(
        default=20,
        description="Maximum keepalive connections"
    )

    # Retry settings
    max_retries: int = Field(
        default=3,
        description="Maximum retry attempts for failed requests"
    )
    retry_min_wait: float = Field(
        default=1.0,
        description="Minimum wait time between retries (seconds)"
    )
    retry_max_wait: float = Field(
        default=10.0,
        description="Maximum wait time between retries (seconds)"
    )

    # Features
    use_python_analysis: bool = Field(
        default=False,
        description="Enable Python analysis tools (remote and local)"
    )

    model_config = {
        "env_prefix": "NL_OPENDATA_MCP_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
