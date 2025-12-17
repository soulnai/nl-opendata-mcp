"""
Test suite for the overheid-mcp server.
Tests all tools including the new DuckDB and full dataset features.
Updated to work with async tools and Pydantic input models.
"""
import asyncio
import json
import os
import duckdb

# Import the server module to access tools and input models
from nl_opendata_mcp import server
from nl_opendata_mcp.models import (
    ListDatasetsInput,
    SearchDatasetsInput,
    SearchField,
    DatasetIdInput,
    SaveDatasetInput,
    SaveToDuckDBInput,
    AnalyzeRemoteInput,
    QueryDatasetInput,
    GetMetadataInput,
    MetadataType,
)
from nl_opendata_mcp.config import get_settings
from nl_opendata_mcp.services.cache import catalog_cache, dataset_cache

# Base URL for direct API testing
DATA_BASE_URL = "https://opendata.cbs.nl/ODataApi/OData"


class MockContext:
    """Mock context for testing FastMCP tools"""
    def info(self, msg):
        print(f"[INFO] {msg}")
    def error(self, msg):
        print(f"[ERROR] {msg}")
    def warning(self, msg):
        print(f"[WARNING] {msg}")


ctx = MockContext()


def get_fn(tool):
    """Extract the callable function from a FunctionTool wrapper"""
    if hasattr(tool, 'fn'):
        return tool.fn
    return tool


async def test_list_datasets():
    print("Testing cbs_list_datasets...")
    fn = get_fn(server.cbs_list_datasets)
    params = ListDatasetsInput(top=2)
    result = await fn(ctx, params)
    print("Success! Result length:", len(result))
    print("Preview:", result[:100].replace('\n', ' '))


async def test_search_datasets():
    print("\nTesting cbs_search_datasets...")
    fn = get_fn(server.cbs_search_datasets)
    params = SearchDatasetsInput(query="Bevolking", top=2)
    result = await fn(ctx, params)
    print(f"Success! Result length:", len(result))
    print("Preview:", result[:100].replace('\n', ' '))


async def test_search_datasets_with_field():
    print("\nTesting cbs_search_datasets with search_field parameter...")
    fn = get_fn(server.cbs_search_datasets)
    params = SearchDatasetsInput(query="Bevolking", top=2, search_field=SearchField.SUMMARY)
    result = await fn(ctx, params)
    print(f"Success! Result length:", len(result))
    print("Preview:", result[:100].replace('\n', ' '))


async def test_estimate_dataset_size():
    print("\nTesting cbs_estimate_dataset_size...")
    fn = get_fn(server.cbs_estimate_dataset_size)
    params = DatasetIdInput(dataset_id="85313NED")
    result = await fn(ctx, params)
    print("Success! Result:")
    print(result)


async def test_get_metadata():
    print("\nTesting cbs_get_metadata (unified)...")
    fn = get_fn(server.cbs_get_metadata)
    params = GetMetadataInput(dataset_id="85313NED", metadata_type=MetadataType.INFO)
    result = await fn(ctx, params)
    print("Success! Result length:", len(result))
    print("Preview:", result[:100].replace('\n', ' '))


async def test_save_dataset():
    print("\nTesting cbs_save_dataset...")
    fn = get_fn(server.cbs_save_dataset)
    path = "test_dataset.csv"
    params = SaveDatasetInput(dataset_id="85313NED", file_name=path, top=50)
    result = await fn(ctx, params)
    print("Success!", result)
    full_path = os.path.join(server.settings.downloads_path, path)
    if os.path.exists(full_path):
        os.remove(full_path)
        print("Cleaned up test file")


async def test_save_dataset_cache():
    print("\nTesting cbs_save_dataset caching...")
    fn = get_fn(server.cbs_save_dataset)
    settings = get_settings()
    path = "test_cache_dataset.csv"

    # Ensure clean state
    full_path = os.path.abspath(os.path.join(settings.downloads_path, path))
    if os.path.exists(full_path):
        os.remove(full_path)

    # Clear cache entry if exists
    dataset_cache.remove(full_path)

    # First Call - Should download
    print("  1. First save (should download)...")
    params = SaveDatasetInput(dataset_id="85313NED", file_name=path, top=10)
    result1 = await fn(ctx, params)
    print("  Result 1:", result1)

    if "cached" in result1:
        print("  [FAIL] First call should not be cached!")
    else:
        print("  [PASS] First call downloaded.")

    # Second Call - Should be cached
    print("  2. Second save (should cache)...")
    result2 = await fn(ctx, params)
    print("  Result 2:", result2)

    if "cached" in result2:
        print("  [PASS] Second call was cached.")
    else:
        print("  [FAIL] Second call was NOT cached!")

    # Clean up
    if os.path.exists(full_path):
        os.remove(full_path)
    dataset_cache.remove(full_path)
    print("  Cleaned up test file and cache")


async def test_save_dataset_to_duckdb():
    print("\nTesting cbs_save_dataset_to_duckdb...")
    fn = get_fn(server.cbs_save_dataset_to_duckdb)
    db_path = "datasets.db"  # Using default db path from server
    table_name = "education_test"

    params = SaveToDuckDBInput(dataset_id="85313NED", table_name=table_name, fetch_all=False)
    result = await fn(ctx, params)
    print("Success!", result)

    if os.path.exists(db_path):
        con = duckdb.connect(db_path)
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"  Tables in DB: {tables}")
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  Rows in {table_name}: {count}")
        con.close()
        os.remove(db_path)
        print("  Cleaned up test database")


async def test_query_dataset():
    print("\nTesting cbs_query_dataset...")
    fn = get_fn(server.cbs_query_dataset)
    params = QueryDatasetInput(dataset_id="85313NED", top=2)
    result = await fn(ctx, params)
    print("Success!", result[:200] if len(result) > 200 else result)


async def test_analyze_dataset():
    print("\nTesting cbs_analyze_remote_dataset...")
    fn = get_fn(server.cbs_analyze_remote_dataset)
    code = """
print(f"Row count: {len(df)}")
print("Columns:", df.columns.tolist())
result = df.describe().to_string()
"""
    params = AnalyzeRemoteInput(dataset_id="85313NED", analysis_code=code)
    result = await fn(ctx, params)
    print("Analysis Result:")
    print(result[:500] + "..." if len(result) > 500 else result)


def test_generate_odata_filter():
    print("\nTesting generate_odata_filter prompt...")
    fn = get_fn(server.generate_odata_filter)
    table_structure = '{"columns": [{"name": "Age", "type": "int"}, {"name": "Population", "type": "int"}]}'
    user_query = "Population in 2023"
    prompt = fn(table_structure, user_query)
    print("Generated Prompt Preview:")
    print(prompt[:200] + "...")


async def run_all_tests():
    print("=" * 60)
    print("RUNNING ALL TESTS")
    print("=" * 60)

    # Core functionality tests
    await test_list_datasets()
    await test_search_datasets()

    print("\n--- Re-running search to test cache ---")
    await test_search_datasets()

    await test_search_datasets_with_field()
    await test_estimate_dataset_size()
    await test_get_metadata()
    await test_query_dataset()
    await test_analyze_dataset()
    test_generate_odata_filter()

    # New tests for save functionality
    await test_save_dataset()
    await test_save_dataset_cache()
    await test_save_dataset_to_duckdb()

    print("\n" + "=" * 60)
    print("ALL TESTS COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_all_tests())
