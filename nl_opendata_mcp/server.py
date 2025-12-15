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
    QueryMetadataInput,
    QueryDatasetInput,
    GetMetadataInput,
    DimensionLookupInput,
)

# Import all tool implementations
from nl_opendata_mcp.tools.discovery import (
    cbs_list_datasets as _cbs_list_datasets,
    cbs_search_datasets as _cbs_search_datasets,
    cbs_check_dataset_availability as _cbs_check_dataset_availability,
)
from nl_opendata_mcp.tools.metadata import (
    cbs_get_dataset_info as _cbs_get_dataset_info,
    cbs_get_table_structure as _cbs_get_table_structure,
    cbs_get_dataset_metadata as _cbs_get_dataset_metadata,
    cbs_query_dataset_metadata as _cbs_query_dataset_metadata,
    cbs_get_metadata as _cbs_get_metadata,
    cbs_get_dimension_values as _cbs_get_dimension_values,
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
# Logging is configured in main() based on settings
logger = logging.getLogger(__name__)

# Get settings
settings = get_settings()

# Initialize FastMCP server
mcp = FastMCP(name="cbs_mcp", include_fastmcp_meta=False)


# ============================================================================
# Tool Registrations
# ============================================================================

@mcp.tool(
    name="cbs_list_datasets",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True
    }
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

    Example:
        - Use when: "Show me available datasets" -> params with top=20
        - Use when: "Get next page of datasets" -> params with skip=20
    """
    return await _cbs_list_datasets(ctx, params)


@mcp.tool(
    name="cbs_search_datasets",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True
    }
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
        str: CSV string containing matching datasets with columns: Identifier, Title, Summary

    Example:
        - Use when: "Find datasets about population" -> query="bevolking"
        - Use when: "Search for inflation data" -> query="inflatie"
    """
    return await _cbs_search_datasets(ctx, params)


@mcp.tool(
    name="cbs_check_dataset_availability",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True
    }
)
async def cbs_check_dataset_availability(ctx: Context, params: DatasetIdInput) -> str:
    """
    Checks if a dataset is available via CBS OData (queryable) or data.overheid.nl (download-only).

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '83583NED' or 'groningen-parkeervakken')

    Returns:
        str: Availability status and source information
    """
    return await _cbs_check_dataset_availability(ctx, params)


@mcp.tool(
    name="cbs_estimate_dataset_size",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True
    }
)
async def cbs_estimate_dataset_size(ctx: Context, params: DatasetIdInput) -> str:
    """
    Estimates the size of a dataset before fetching. Use to plan your query strategy.

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')

    Returns:
        str: Size estimation with row count, column count, and recommended fetch strategy

    Example:
        - Use when: "How big is dataset 85313NED?" -> dataset_id="85313NED"
    """
    return await _cbs_estimate_dataset_size(ctx, params)


@mcp.tool(
    name="cbs_save_dataset",
    annotations={
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True
    }
)
async def cbs_save_dataset(ctx: Context, params: SaveDatasetInput) -> str:
    """
    Saves a dataset to a CSV file.

    Args:
        params: SaveDatasetInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')
            - file_name (str): File name to save the dataset. Default: downloads/{dataset_id}.csv
            - top (int): Records per request (default: 1000, only if fetch_all=False)
            - skip (int): Records to skip (default: 0, only if fetch_all=False)
            - fetch_all (bool): Fetch all records with pagination (default: False)
            - translate (bool): Translate coded values to human-readable text (default: True)

    Returns:
        str: Success message with file path and record count, or error message
    """
    return await _cbs_save_dataset(ctx, params)


@mcp.tool(
    name="cbs_save_dataset_to_duckdb",
    annotations={
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def cbs_save_dataset_to_duckdb(ctx: Context, params: SaveToDuckDBInput) -> str:
    """
    Saves a dataset to a DuckDB database for efficient querying and analysis.

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


@mcp.tool(
    name="cbs_list_local_datasets",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False
    }
)
async def cbs_list_local_datasets(ctx: Context) -> str:
    """
    Lists all locally saved CSV datasets in the downloads directory.

    Call this BEFORE cbs_analyze_local_dataset to see what files are available.

    Returns:
        str: List of CSV files with sizes and row counts.

    Workflow:
        1. Call cbs_list_local_datasets to see available files
        2. Call cbs_analyze_local_dataset with the exact filename
    """
    return await _cbs_list_local_datasets(ctx)

if settings.use_python_analysis:
    @mcp.tool(
        name="cbs_analyze_remote_dataset",
        annotations={
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True
        }
    )
    async def cbs_analyze_remote_dataset(ctx: Context, params: AnalyzeRemoteInput) -> str:
        """
        Analyzes a remote dataset using Python/Pandas. Fetches data and executes analysis code.

        Args:
            params: AnalyzeRemoteInput containing:
                - dataset_id (str): Dataset ID (e.g., '85313NED')
                - analysis_code (str, optional): Python code to execute on 'df' DataFrame
                - script_path (str, optional): Path to .py file with analysis code (preferred for complex analysis)
                - filter (str, optional): OData filter to apply BEFORE fetching (e.g., "Perioden eq '2023JJ00'")
                - select (List[str], optional): Column names to fetch (reduces data transfer)
                - top (int): Maximum records to fetch (default: 10000)
                - translate (bool): Translate coded values to human-readable text (default: True)

        IMPORTANT - Translation Behavior:
            When translate=True (default), dimension values are converted to human-readable text.
            - OData filter uses RAW codes: filter="Luchthavens eq 'A043591'"
            - DataFrame contains TRANSLATED values: df['Luchthavens'] == 'Eindhoven Airport'
            Use cbs_get_dimension_values to find the correct codes for filtering.

        Returns:
            str: Analysis result. Print output or assign to 'result' variable.

        Available in code: df (DataFrame), pd (pandas), np (numpy), matplotlib, seaborn.

        CHARTING: Use this tool to create and save charts! Import matplotlib.pyplot, create the plot,
        then save with plt.savefig('chart.png'). Do NOT use plt.show() - save to file instead.

        Example:
            analysis_code="print(df.describe())" or script_path="analysis.py"

        Chart Example:
            analysis_code=\"\"\"
            import matplotlib.pyplot as plt
            df.plot(kind='bar', x='Column1', y='Column2')
            plt.title('My Chart')
            plt.savefig('chart.png', dpi=150, bbox_inches='tight')
            plt.close()
            print('Chart saved to chart.png')
            \"\"\"
        """
        return await _cbs_analyze_remote_dataset(ctx, params)


    @mcp.tool(
        name="cbs_analyze_local_dataset",
        annotations={
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True
        }
    )
    async def cbs_analyze_local_dataset(ctx: Context, params: AnalyzeLocalInput) -> str:
        """
        Analyzes a local CSV dataset using Python/Pandas code.

        IMPORTANT: First call cbs_list_local_datasets to see available files!

        Args:
            params: AnalyzeLocalInput containing:
                - dataset_name (str): Filename from downloads folder (e.g., 'population.csv')
                - analysis_code (str): Python code to execute. Must use print() for output.
                - script_path (str, optional): Path to .py file with analysis code

        Available in code: df (DataFrame), pd (pandas), np (numpy), matplotlib, seaborn.

        CHARTING: Use this tool to create and save charts! Import matplotlib.pyplot, create the plot,
        then save with plt.savefig('chart.png'). Do NOT use plt.show() - save to file instead.

        Returns:
            str: Output from print() statements in your code.

        Examples:
            - dataset_name="data.csv", analysis_code="print(df.head())"
            - dataset_name="data.csv", analysis_code="print(df.describe())"
            - dataset_name="data.csv", analysis_code="print(df.groupby('Col').sum())"

        Chart Example:
            analysis_code=\"\"\"
            import matplotlib.pyplot as plt
            df.plot(kind='line', x='Year', y='Population')
            plt.title('Population Over Time')
            plt.savefig('population_chart.png', dpi=150, bbox_inches='tight')
            plt.close()
            print('Chart saved to population_chart.png')
            \"\"\"
        """
        return await _cbs_analyze_local_dataset(ctx, params)


@mcp.tool(
    name="cbs_get_dataset_info",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True
    }
)
async def cbs_get_dataset_info(ctx: Context, params: DatasetIdInput) -> str:
    """
    Gets detailed information and description for a specific dataset (TableInfos).

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')

    Returns:
        str: CSV string containing dataset metadata
    """
    return await _cbs_get_dataset_info(ctx, params)


@mcp.tool(
    name="cbs_get_table_structure",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True
    }
)
async def cbs_get_table_structure(ctx: Context, params: DatasetIdInput) -> str:
    """
    Gets the structure (columns and data types) of a dataset (DataProperties).

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')

    Returns:
        str: CSV string containing column definitions (Key, Type, Title, Description)
    """
    return await _cbs_get_table_structure(ctx, params)


@mcp.tool(
    name="cbs_get_dataset_metadata",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True
    }
)
async def cbs_get_dataset_metadata(ctx: Context, params: DatasetIdInput) -> str:
    """
    Gets metadata and classification links for a dataset.

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')

    Returns:
        str: JSON string containing dataset metadata and available endpoints
    """
    return await _cbs_get_dataset_metadata(ctx, params)


@mcp.tool(
    name="cbs_query_dataset_metadata",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True
    }
)
async def cbs_query_dataset_metadata(ctx: Context, params: QueryMetadataInput) -> str:
    """
    Queries specific metadata endpoint for a dataset.

    Args:
        params: QueryMetadataInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')
            - metadata_name (str): Metadata type (e.g., 'DataProperties', 'Geslacht')

    Returns:
        str: JSON string containing the requested metadata
    """
    return await _cbs_query_dataset_metadata(ctx, params)


@mcp.tool(
    name="cbs_get_metadata",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True
    }
)
async def cbs_get_metadata(ctx: Context, params: GetMetadataInput) -> str:
    """
    Unified tool to get dataset metadata. Consolidates info, structure, and endpoints retrieval.

    Args:
        params: GetMetadataInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')
            - metadata_type (str): Type of metadata:
                - 'info': Dataset description and details (TableInfos)
                - 'structure': Column definitions and types (DataProperties)
                - 'endpoints': Available metadata endpoints
                - 'custom': Custom endpoint (specify custom_endpoint)
            - custom_endpoint (str, optional): Custom endpoint name for metadata_type='custom'

    Returns:
        str: CSV for info/structure, JSON for endpoints/custom

    Example:
        - Get dataset info: metadata_type="info"
        - Get columns: metadata_type="structure"
        - Get gender categories: metadata_type="custom", custom_endpoint="Geslacht"
    """
    return await _cbs_get_metadata(ctx, params)


@mcp.tool(
    name="cbs_get_dimension_values",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True
    }
)
async def cbs_get_dimension_values(ctx: Context, params: DimensionLookupInput) -> str:
    """
    Gets all possible values for a dimension with their codes and descriptions.

    Use this tool to find the correct codes for OData filtering. CBS datasets use
    coded values (e.g., 'A043591') that map to human-readable names (e.g., 'Eindhoven Airport').

    Args:
        params: DimensionLookupInput containing:
            - dataset_id (str): Dataset ID (e.g., '37478hvv')
            - dimension_name (str): Dimension name from DataProperties (e.g., 'Luchthavens', 'Geslacht')

    Returns:
        str: Table of dimension values with Code, Title, and Description columns.
             Use the 'Code' column values for OData filters.

    Example:
        Input: dataset_id="37478hvv", dimension_name="Luchthavens"
        Then use in filter: filter="Luchthavens eq 'A043591'"

    Common dimension names:
        - Geslacht: Gender (Mannen, Vrouwen, Totaal)
        - Perioden: Time periods (2023JJ00 = year, 2023MM01 = month)
        - RegioS: Regions
        - Leeftijd: Age groups
    """
    return await _cbs_get_dimension_values(ctx, params)


@mcp.tool(
    name="cbs_query_dataset",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True
    }
)
async def cbs_query_dataset(ctx: Context, params: QueryDatasetInput) -> str:
    """
    Queries data from a dataset with optional filtering and column selection.

    Dimension values are automatically translated to human-readable text by default.
    For example, Geslacht code "3000" becomes "Mannen", Perioden "2023JJ00" becomes "2023".

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

    Example:
        - Use when: "Get population data for 2023" -> filter="Perioden eq '2023JJ00'"
    """
    return await _cbs_query_dataset(ctx, params)


@mcp.tool(
    name="cbs_inspect_dataset_details",
    annotations={
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True
    }
)
async def cbs_inspect_dataset_details(ctx: Context, params: DatasetIdInput) -> str:
    """
    Inspects a dataset providing comprehensive summary: metadata, structure, and sample data.
    One-stop-shop tool to save tokens and roundtrips.

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')

    Returns:
        str: Formatted report with source, metadata, columns, and sample data (CSV)
    """
    return await _cbs_inspect_dataset_details(ctx, params)


# ============================================================================
# Prompts
# ============================================================================

@mcp.prompt()
def generate_odata_filter(table_structure: str, user_query: str) -> str:
    """
    Generates an OData filter string based on the table structure and user query.

    Args:
        table_structure: The JSON string returned by cbs_get_table_structure.
        user_query: The natural language query from the user (e.g., "Population in 2023").

    Returns:
        A prompt for the LLM to generate the OData filter.
    """
    return f"""You are an expert in OData V3 filtering.

Based on the following table structure:
{table_structure}

And the user's request:
"{user_query}"

Please generate a valid OData V3 filter string (for the $filter parameter).
- Use 'eq' for exact matches.
- Use 'substringof' for partial string matches (e.g. substringof('text', Field)).
- Use 'and' / 'or' for logical operations.
- Format dates correctly if applicable.
- Return ONLY the filter string, nothing else.
"""


@mcp.prompt()
def collect_dataset_metadata(dataset_id: str, user_query: str) -> str:
    """
    Collects metadata for a specific dataset.

    Args:
        dataset_id: The ID of the dataset (e.g., '85313NED')
        user_query: The natural language query from the user.

    Returns:
        A prompt for metadata collection workflow.
    """
    return f"""You are an expert in OData V3 metadata collection. This is required to display data in a human readable way (to convert codes into values).

Based on the following dataset ID:
{dataset_id}
And the user's request:
"{user_query}"

Please collect the metadata for the dataset. First collect the metadata for the dataset using the cbs_get_dataset_metadata tool.
"""


@mcp.prompt()
def explore_dataset(user_query: str) -> str:
    """
    Explores a specific dataset based on the user query.

    Args:
        user_query: The natural language query from the user (e.g., "Calculate the average population").

    Returns:
        A prompt for the LLM to explore the dataset.
    """
    return f"""You are an expert in exploring datasets.

Based on the following user query:
"{user_query}"

Find the most suitable datasets for the user query using the cbs_get_dataset_metadata tool.
Then collect the metadata for the dataset using the cbs_get_dataset_metadata tool.
Explore dataset metadata using cbs_get_dataset_metadata tool.
Explore dataset fields using cbs_query_dataset tool with low top value.

Return the dataset ID and the dataset metadata for further processing.
"""


@mcp.prompt()
def analyze_local_dataset_guide(analysis_goal: str) -> str:
    """
    Guide for analyzing a local CSV dataset with pandas.

    Args:
        analysis_goal: What the user wants to analyze (e.g., "find trends in population data")

    Returns:
        A detailed prompt for performing local dataset analysis.
    """
    return f"""You are a data analyst helping analyze a local CSV dataset.

USER'S GOAL: "{analysis_goal}"

WORKFLOW - Follow these steps in order:

## Step 1: List Available Files
First, call cbs_list_local_datasets (no parameters needed) to see what CSV files exist.
This will show you filenames, sizes, and row counts.

## Step 2: Explore the Data Structure
Call cbs_analyze_local_dataset with:
- dataset_name: exact filename from Step 1 (e.g., "population.csv")
- analysis_code: "print(df.columns.tolist())"

Then call again with:
- analysis_code: "print(df.dtypes)"

And to see sample data:
- analysis_code: "print(df.head())"

## Step 3: Perform Analysis
Now write analysis code based on the user's goal. Always use print() for output.

COMMON ANALYSIS PATTERNS:

# Basic statistics
print(df.describe())

# Value counts for a column
print(df['ColumnName'].value_counts())

# Group by and aggregate
print(df.groupby('Category')['Value'].sum())

# Filter rows
filtered = df[df['Year'] == 2023]
print(filtered.head())

# Multiple aggregations
result = df.groupby('Region').agg({{
    'Population': ['mean', 'sum'],
    'Area': 'first'
}})
print(result)

# Correlation
print(df[['Col1', 'Col2', 'Col3']].corr())

IMPORTANT RULES:
1. ALWAYS use print() - code without print() produces no output
2. The DataFrame is named 'df' - don't try to load files yourself
3. pandas is available as 'pd', numpy as 'np'
4. For large outputs, use .head() or .tail() to limit rows
5. Check column names first - CBS data often has Dutch column names

Now help the user achieve their analysis goal: "{analysis_goal}"
"""


@mcp.prompt()
def generate_chart(dataset_id: str, chart_request: str) -> str:
    """
    Guide for creating and saving charts/visualizations from CBS data.

    Args:
        dataset_id: The CBS dataset ID (e.g., '85313NED')
        chart_request: What kind of chart the user wants (e.g., "bar chart of population by region")

    Returns:
        A detailed prompt for creating charts with the analyze tools.
    """
    return f"""You are a data visualization expert. Create a chart based on the user's request.

DATASET: {dataset_id}
USER REQUEST: "{chart_request}"

## WORKFLOW - Follow these steps:

### Step 1: Explore the Data
First, use cbs_analyze_remote_dataset to understand the data structure:
```
dataset_id="{dataset_id}"
analysis_code="print(df.columns.tolist())\\nprint(df.head())"
```

### Step 2: Create and Save the Chart
Use cbs_analyze_remote_dataset with chart code:

```python
import matplotlib.pyplot as plt
import seaborn as sns

# Set style for better looking charts
plt.style.use('seaborn-v0_8-whitegrid')
plt.figure(figsize=(10, 6))

# === YOUR CHART CODE HERE ===
# Examples:

# Bar chart:
# df.plot(kind='bar', x='Column1', y='Column2')

# Line chart:
# df.plot(kind='line', x='Year', y='Value')

# Pie chart:
# df.groupby('Category')['Value'].sum().plot(kind='pie', autopct='%1.1f%%')

# Seaborn bar:
# sns.barplot(data=df, x='Category', y='Value')

# Seaborn line:
# sns.lineplot(data=df, x='Year', y='Value', hue='Group')

# === END CHART CODE ===

plt.title('Your Chart Title')
plt.xlabel('X Label')
plt.ylabel('Y Label')
plt.tight_layout()

# IMPORTANT: Save to file, don't use plt.show()
plt.savefig('chart.png', dpi=150, bbox_inches='tight')
plt.close()

print('Chart saved to chart.png')
```

## CHART TYPE GUIDE:

| User Request | Chart Type | Code |
|--------------|------------|------|
| "bar chart" | Bar | `df.plot(kind='bar', x='col1', y='col2')` |
| "line chart", "trend" | Line | `df.plot(kind='line', x='year', y='value')` |
| "pie chart" | Pie | `df.groupby('cat')['val'].sum().plot(kind='pie')` |
| "histogram" | Histogram | `df['column'].hist(bins=20)` |
| "scatter plot" | Scatter | `df.plot.scatter(x='col1', y='col2')` |
| "heatmap" | Heatmap | `sns.heatmap(df.corr())` |

## IMPORTANT RULES:
1. ALWAYS use plt.savefig() to save the chart - never use plt.show()
2. ALWAYS call plt.close() after saving to free memory
3. ALWAYS print() the file path so the user knows where to find the chart
4. Use descriptive filenames (e.g., 'population_by_region.png')
5. Add title and axis labels for clarity
6. Use dpi=150 or higher for good quality

Now create the chart the user requested: "{chart_request}"
"""


# ============================================================================
# Lifecycle Management
# ============================================================================

async def initialize_server():
    """Initialize resources on server startup."""
    logger.info("Initializing overheid-mcp server...")
    # Ensure downloads directory exists
    ensure_directory_exists(settings.downloads_path)
    logger.info(f"Downloads directory: {os.path.abspath(settings.downloads_path)}")
    logger.info("Server initialization complete")


async def cleanup_server():
    """Cleanup resources on server shutdown."""
    logger.info("Cleaning up overheid-mcp server...")
    await HTTPClientManager.close()
    logger.info("HTTP client closed")
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

    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, log_level.upper(), logging.ERROR))

    logger.info(f"Starting server with transport={transport}, log_level={log_level}")

    # Initialize on startup using modern asyncio API
    asyncio.run(initialize_server())

    # Register cleanup on exit
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
