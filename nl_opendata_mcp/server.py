"""
NL OpenData MCP Server - Main entry point.

This module provides the FastMCP server that exposes CBS and data.overheid.nl
tools for accessing Dutch government open data.
"""
from fastmcp import FastMCP, Context
import logging
import os

from nl_opendata_mcp.config import get_settings
from nl_opendata_mcp.services.http_client import HTTPClientManager
from nl_opendata_mcp.services.cache import catalog_cache
from nl_opendata_mcp.utils import ensure_directory_exists

# Import all models
from nl_opendata_mcp.models import (
    ListDatasetsInput,
    SearchDatasetsInput,
    DatasetIdInput,
    SaveDatasetInput,
    SaveToDuckDBInput,
    AnalyzeRemoteInput,
    AnalyzeLocalInput,
    QueryDatasetInput,
    GetMetadataInput,
)

# Import all tool implementations
from nl_opendata_mcp.tools.discovery import (
    cbs_list_datasets as _cbs_list_datasets,
    cbs_search_datasets as _cbs_search_datasets,
    cbs_check_dataset_availability as _cbs_check_dataset_availability,
)
from nl_opendata_mcp.tools.metadata import (
    cbs_get_metadata as _cbs_get_metadata,
)
from nl_opendata_mcp.tools.query import (
    cbs_query_dataset as _cbs_query_dataset,
    cbs_estimate_dataset_size as _cbs_estimate_dataset_size,
    cbs_inspect_dataset_details as _cbs_inspect_dataset_details,
)
from nl_opendata_mcp.tools.export import (
    cbs_save_dataset as _cbs_save_dataset,
    cbs_save_dataset_to_duckdb as _cbs_save_dataset_to_duckdb,
)
from nl_opendata_mcp.tools.analysis import (
    cbs_analyze_remote_dataset as _cbs_analyze_remote_dataset,
    cbs_analyze_local_dataset as _cbs_analyze_local_dataset,
    cbs_list_local_datasets as _cbs_list_local_datasets,
)

# Configure logging
logger = logging.getLogger(__name__)

# Get settings
settings = get_settings()

# Initialize FastMCP server
mcp = FastMCP(name="cbs_mcp", include_fastmcp_meta=False)


# ============================================================================
# Tool Registrations - DISCOVERY
# ============================================================================

@mcp.tool(
    name="cbs_list_datasets",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
)
async def cbs_list_datasets(ctx: Context, params: ListDatasetsInput) -> str:
    """
    Lists available datasets from the CBS OData Catalog.

    Args:
        params: ListDatasetsInput containing:
            - top (int): Number of records to return (default: 10, max: 1000)
            - skip (int): Number of records to skip (default: 0)

    Returns:
        str: CSV string containing dataset list with columns: Identifier, Title, Summary
    """
    return await _cbs_list_datasets(ctx, params)


@mcp.tool(
    name="cbs_search_datasets",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
)
async def cbs_search_datasets(ctx: Context, params: SearchDatasetsInput) -> str:
    """
    Searches for datasets in the CBS OData Catalog by keyword.

    Args:
        params: SearchDatasetsInput containing:
            - query (str): Search term (e.g., "Bevolking", "Inflation")
            - top (int): Number of records to return (default: 10)
            - skip (int): Number of records to skip (default: 0)
            - search_field (str): Where to search - "all", "title", or "summary"

    Returns:
        str: CSV string containing matching datasets
    """
    return await _cbs_search_datasets(ctx, params)


@mcp.tool(
    name="cbs_check_dataset_availability",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
)
async def cbs_check_dataset_availability(ctx: Context, params: DatasetIdInput) -> str:
    """
    Checks if a dataset is available via CBS OData (queryable) or data.overheid.nl (download-only).

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '83583NED')

    Returns:
        str: Availability status and source information
    """
    return await _cbs_check_dataset_availability(ctx, params)


# ============================================================================
# Tool Registrations - METADATA (Consolidated)
# ============================================================================

@mcp.tool(
    name="cbs_inspect_dataset_details",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
)
async def cbs_inspect_dataset_details(ctx: Context, params: DatasetIdInput) -> str:
    """
    Compact dataset overview: title, dimensions, measures, and sample data.
    Use this first to understand a dataset's structure.

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')

    Returns:
        str: Compact report with title, column list, and 3-row sample
    """
    return await _cbs_inspect_dataset_details(ctx, params)


@mcp.tool(
    name="cbs_get_metadata",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
)
async def cbs_get_metadata(ctx: Context, params: GetMetadataInput) -> str:
    """
    Unified metadata tool for detailed info, structure, dimension values, or custom endpoints.

    Args:
        params: GetMetadataInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')
            - metadata_type (str): Type of metadata:
                - 'info': Dataset description (TableInfos)
                - 'structure': Column definitions (DataProperties)
                - 'endpoints': Available metadata endpoints
                - 'dimensions': Dimension values with codes for filtering (requires endpoint_name)
                - 'custom': Custom endpoint query (requires endpoint_name)
            - endpoint_name (str, optional): Required for 'dimensions' and 'custom' types
              (e.g., 'Geslacht', 'Perioden', 'Luchthavens')

    Returns:
        str: CSV for info/structure/dimensions, JSON for endpoints/custom

    Examples:
        - Get columns: metadata_type="structure"
        - Get dimension codes: metadata_type="dimensions", endpoint_name="Geslacht"
        - Get raw endpoint: metadata_type="custom", endpoint_name="CategoryGroups"

    IMPORTANT - Finding Dimension Codes:
        Use metadata_type="dimensions" to find codes for OData filtering.
        CBS uses coded values (e.g., 'A043591') that map to names (e.g., 'Eindhoven Airport').

        Workflow:
        1. Get dimension codes: metadata_type="dimensions", endpoint_name="Luchthavens"
        2. Use code in query: filter="Luchthavens eq 'A043591'"
    """
    return await _cbs_get_metadata(ctx, params)


# ============================================================================
# Tool Registrations - QUERY
# ============================================================================

@mcp.tool(
    name="cbs_query_dataset",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
)
async def cbs_query_dataset(ctx: Context, params: QueryDatasetInput) -> str:
    """
    Queries data from a dataset with optional filtering and column selection.

    Args:
        params: QueryDatasetInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')
            - top (int): Number of records (default: 10)
            - skip (int): Records to skip (default: 0)
            - filter (str, optional): OData filter (e.g., "Perioden eq '2023JJ00'")
            - select (List[str], optional): Column names to return
            - compact (bool): Return summary for large results (default: True)
            - translate (bool): Translate coded dimension values to text (default: True)

    Returns:
        str: CSV data with human-readable dimension values

    Note: Use cbs_get_metadata with metadata_type="dimensions" to find filter codes.
    """
    return await _cbs_query_dataset(ctx, params)


@mcp.tool(
    name="cbs_estimate_dataset_size",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
)
async def cbs_estimate_dataset_size(ctx: Context, params: DatasetIdInput) -> str:
    """
    Estimates the size of a dataset before fetching.

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')

    Returns:
        str: Size estimation with row count, column count, and recommended fetch strategy
    """
    return await _cbs_estimate_dataset_size(ctx, params)


# ============================================================================
# Tool Registrations - EXPORT
# ============================================================================

@mcp.tool(
    name="cbs_save_dataset",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
)
async def cbs_save_dataset(ctx: Context, params: SaveDatasetInput) -> str:
    """
    Saves a dataset to a CSV file.

    Args:
        params: SaveDatasetInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')
            - file_name (str): File name to save the dataset
            - top (int): Records per request (default: 1000)
            - skip (int): Records to skip (default: 0)
            - fetch_all (bool): Fetch all records with pagination (default: False)
            - translate (bool): Translate coded values to text (default: True)

    Returns:
        str: Success message with file path and record count
    """
    return await _cbs_save_dataset(ctx, params)


@mcp.tool(
    name="cbs_save_dataset_to_duckdb",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False}
)
async def cbs_save_dataset_to_duckdb(ctx: Context, params: SaveToDuckDBInput) -> str:
    """
    Saves a dataset to a DuckDB database for efficient querying.

    Args:
        params: SaveToDuckDBInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')
            - table_name (str, optional): Table name (default: dataset_id)
            - fetch_all (bool): Fetch all records (default: True)
            - select (List[str], optional): Column names to fetch

    Returns:
        str: Success message with database path, table name, and row count
    """
    return await _cbs_save_dataset_to_duckdb(ctx, params)


# ============================================================================
# Tool Registrations - ANALYSIS (conditional)
# ============================================================================

@mcp.tool(
    name="cbs_list_local_datasets",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": False}
)
async def cbs_list_local_datasets(ctx: Context) -> str:
    """
    Lists all locally saved CSV datasets in the downloads directory.

    Returns:
        str: List of CSV files with sizes and row counts.
    """
    return await _cbs_list_local_datasets(ctx)


if settings.use_python_analysis:
    @mcp.tool(
        name="cbs_analyze_remote_dataset",
        annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
    )
    async def cbs_analyze_remote_dataset(ctx: Context, params: AnalyzeRemoteInput) -> str:
        """
        Analyzes a remote dataset using Python/Pandas. Fetches data and executes analysis code.

        Args:
            params: AnalyzeRemoteInput containing:
                - dataset_id (str): Dataset ID (e.g., '85313NED')
                - analysis_code (str, optional): Python code to execute on 'df' DataFrame
                - script_path (str, optional): Path to .py file with analysis code
                - filter (str, optional): OData filter to apply BEFORE fetching
                - select (List[str], optional): Column names to fetch
                - top (int): Maximum records to fetch (default: 10000)
                - translate (bool): Translate coded values to text (default: True)

        IMPORTANT - Translation Behavior:
            OData filter uses RAW codes: filter="Luchthavens eq 'A043591'"
            DataFrame contains TRANSLATED values: df['Luchthavens'] == 'Eindhoven Airport'
            Use cbs_get_metadata(type="dimensions") to find codes.

        Returns:
            str: Analysis result. Print output or assign to 'result' variable.

        Available in code: df (DataFrame), pd (pandas), np (numpy), matplotlib, seaborn.

        CHARTING: Save charts with plt.savefig('chart.png'). Do NOT use plt.show().
        """
        return await _cbs_analyze_remote_dataset(ctx, params)


    @mcp.tool(
        name="cbs_analyze_local_dataset",
        annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True}
    )
    async def cbs_analyze_local_dataset(ctx: Context, params: AnalyzeLocalInput) -> str:
        """
        Analyzes a local CSV dataset using Python/Pandas code.

        Args:
            params: AnalyzeLocalInput containing:
                - dataset_name (str): Filename from downloads folder (e.g., 'population.csv')
                - analysis_code (str): Python code to execute. Must use print() for output.
                - script_path (str, optional): Path to .py file with analysis code

        Returns:
            str: Output from print() statements in your code.

        Available in code: df (DataFrame), pd (pandas), np (numpy), matplotlib, seaborn.

        CHARTING: Save charts with plt.savefig('chart.png'). Do NOT use plt.show().
        """
        return await _cbs_analyze_local_dataset(ctx, params)


# ============================================================================
# Prompts
# ============================================================================

@mcp.prompt()
def generate_odata_filter(table_structure: str, user_query: str) -> str:
    """Generates an OData filter string based on the table structure and user query."""
    return f"""You are an expert in OData V3 filtering.

Based on the following table structure:
{table_structure}

And the user's request:
"{user_query}"

Please generate a valid OData V3 filter string (for the $filter parameter).
- Use 'eq' for exact matches.
- Use 'substringof' for partial string matches.
- Use 'and' / 'or' for logical operations.
- Return ONLY the filter string, nothing else.
"""


@mcp.prompt()
def explore_dataset(user_query: str) -> str:
    """Explores a specific dataset based on the user query."""
    return f"""You are an expert in exploring datasets.

Based on the following user query:
"{user_query}"

1. Use cbs_search_datasets to find relevant datasets
2. Use cbs_inspect_dataset_details to understand the dataset structure
3. Use cbs_get_metadata(type="dimensions") to find filter codes if needed
4. Use cbs_query_dataset to fetch data

Return the dataset ID and key findings.
"""


@mcp.prompt()
def generate_chart(dataset_id: str, chart_request: str) -> str:
    """Guide for creating charts/visualizations from CBS data."""
    return f"""You are a data visualization expert.

DATASET: {dataset_id}
REQUEST: "{chart_request}"

WORKFLOW:
1. Use cbs_inspect_dataset_details to understand the data
2. Use cbs_analyze_remote_dataset with chart code

Chart code template:
```python
import matplotlib.pyplot as plt
# Create your chart
df.plot(kind='bar', x='Column1', y='Column2')
plt.title('Title')
plt.tight_layout()
plt.savefig('chart.png', dpi=150, bbox_inches='tight')
plt.close()
print('Chart saved to chart.png')
```

RULES:
1. ALWAYS use plt.savefig() - never plt.show()
2. ALWAYS call plt.close() after saving
3. Use descriptive filenames
"""


# ============================================================================
# Lifecycle Management
# ============================================================================

async def initialize_server():
    """Initialize resources on server startup."""
    logger.info("Initializing overheid-mcp server...")
    ensure_directory_exists(settings.downloads_path)
    logger.info(f"Downloads directory: {os.path.abspath(settings.downloads_path)}")
    logger.info("Server initialization complete")


async def cleanup_server():
    """Cleanup resources on server shutdown."""
    logger.info("Cleaning up overheid-mcp server...")
    await HTTPClientManager.close()
    logger.info("Server cleanup complete")


# ============================================================================
# Main
# ============================================================================

def main():
    """Main entry point for the MCP server."""
    import asyncio
    import atexit

    transport = os.getenv("TRANSPORT", settings.transport)
    log_level = os.getenv("LOG_LEVEL", settings.log_level)

    logging.getLogger().setLevel(getattr(logging, log_level.upper(), logging.ERROR))
    logger.info(f"Starting server with transport={transport}, log_level={log_level}")

    asyncio.run(initialize_server())

    def sync_cleanup():
        try:
            asyncio.run(cleanup_server())
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

    atexit.register(sync_cleanup)
    
    if transport == "http":
        mcp.run(host=settings.host, port=settings.port, transport="http")
    elif transport == "sse":
        mcp.run(host=settings.host, port=settings.port, transport="sse")
    else:
        mcp.run(show_banner=True, log_level=log_level)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        import sys
        print("\nServer stopped by user.", file=sys.stderr)
        sys.exit(0)
