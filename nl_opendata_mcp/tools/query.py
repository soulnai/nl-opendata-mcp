"""
Query tools for fetching and inspecting CBS dataset data.
"""
import logging
import pandas as pd
from fastmcp import Context

from ..config import get_settings
from ..models import DatasetIdInput, QueryDatasetInput
from ..services.cache import catalog_cache
from ..services.http_client import HTTPClientManager, fetch_with_retry
from ..services.translator import translator
from ..utils import (
    handle_http_error,
    validate_dataset_id,
    sanitize_odata_filter,
    sanitize_select_columns,
    ValidationError,
)
from .base import load_catalog_cache

logger = logging.getLogger(__name__)
settings = get_settings()


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
    try:
        dataset_id = validate_dataset_id(params.dataset_id)
    except ValidationError as e:
        return e.to_error_string()

    ctx.info(f"Estimating size for dataset {dataset_id}...")
    logger.info(f"Estimating dataset size: {dataset_id}")

    output = [f"DATASET SIZE ESTIMATE: {dataset_id}", "=" * 40]

    try:
        client = await HTTPClientManager.get_client()

        # Get sample for column info
        sample_url = f"{settings.data_base_url}/{dataset_id}/TypedDataSet?$format=json&$top=1"
        sample_resp = await client.get(sample_url)
        sample_resp.raise_for_status()
        sample_data = sample_resp.json().get('value', [])

        if sample_data:
            columns = list(sample_data[0].keys())
            col_count = len(columns)
            output.append(f"Columns: {col_count}")
            output.append(f"Column names: {', '.join(columns[:10])}{'...' if col_count > 10 else ''}")
        else:
            output.append("No data available in dataset.")
            return "\n".join(output)

        # Estimate rows
        test_url = f"{settings.data_base_url}/{dataset_id}/TypedDataSet?$format=json&$top=1&$skip=100000"
        test_resp = await client.get(test_url)
        test_data = test_resp.json().get('value', [])

        if test_data:
            row_estimate = ">100,000 rows"
            strategy = "LARGE: Use cbs_save_dataset_to_duckdb() or cbs_save_dataset(fetch_all=True) with select parameter"
        else:
            test_url = f"{settings.data_base_url}/{dataset_id}/TypedDataSet?$format=json&$top=1&$skip=10000"
            test_resp = await client.get(test_url)
            test_data = test_resp.json().get('value', [])

            if test_data:
                row_estimate = "10,000 - 100,000 rows"
                strategy = "MEDIUM: Consider using select parameter to reduce columns"
            else:
                test_url = f"{settings.data_base_url}/{dataset_id}/TypedDataSet?$format=json&$top=1&$skip=1000"
                test_resp = await client.get(test_url)
                test_data = test_resp.json().get('value', [])

                if test_data:
                    row_estimate = "1,000 - 10,000 rows"
                    strategy = "MEDIUM: Safe to query with compact=True"
                else:
                    row_estimate = "<1,000 rows"
                    strategy = "SMALL: Safe to return full CSV directly"

        output.append(f"Estimated rows: {row_estimate}")
        output.append("-" * 40)
        output.append(f"Recommended: {strategy}")

        if col_count > 10:
            output.append(f"TIP: Use select=[...] to fetch only needed columns (current: {col_count})")

        return "\n".join(output)

    except Exception as e:
        return handle_http_error(e, "cbs_estimate_dataset_size")


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
            - translate (bool): Translate coded values to text (default: True)

    Returns:
        str: CSV data or compact summary depending on result size

    Example:
        - Use when: "Get population data for 2023" -> filter="Perioden eq '2023JJ00'"
    """
    try:
        dataset_id = validate_dataset_id(params.dataset_id)
        sanitized_filter = sanitize_odata_filter(params.filter)
        sanitized_select = sanitize_select_columns(params.select)
    except ValidationError as e:
        return e.to_error_string()

    url = f"{settings.data_base_url}/{dataset_id}/TypedDataSet?$format=json&$top={params.top}&$skip={params.skip}"
    if sanitized_filter:
        url += f"&$filter={sanitized_filter}"
    if sanitized_select:
        url += f"&$select={','.join(sanitized_select)}"

    ctx.info(f"Querying dataset {dataset_id}: {url}")
    logger.info(f"Querying dataset: {dataset_id}, top={params.top}, skip={params.skip}, has_filter={sanitized_filter is not None}, translate={params.translate}")

    try:
        response = await fetch_with_retry(url)
        data = response.json().get('value', [])

        if not data:
            return "No records found."

        df = pd.DataFrame(data)

        # Auto-translate coded dimension values to human-readable text
        if params.translate:
            try:
                ctx.info(f"Translating dimension values for {dataset_id}...")
                df = await translator.translate_dataframe(df, dataset_id)
            except Exception as e:
                logger.warning(f"Translation failed for {dataset_id}: {e}")
                # Continue with untranslated data

        if params.compact and (len(df) > 100 or len(df.columns) > 10):
            summary = [
                f"QUERY RESULT: {dataset_id}",
                f"Rows: {len(df)}, Columns: {len(df.columns)}",
                f"Columns: {', '.join(df.columns.tolist())}",
                "---",
                "SAMPLE (first 5 rows):",
                df.head(5).to_csv(index=False)
            ]
            return "\n".join(summary)

        return df.to_csv(index=False)

    except Exception as e:
        return handle_http_error(e, "cbs_query_dataset")


async def cbs_inspect_dataset_details(ctx: Context, params: DatasetIdInput) -> str:
    """
    Compact dataset overview: title, columns, dimensions, and sample data.
    Use cbs_get_metadata for detailed info on specific aspects.

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')

    Returns:
        str: Compact report with title, column list, dimensions, and 3-row sample
    """
    try:
        dataset_id = validate_dataset_id(params.dataset_id)
    except ValidationError as e:
        return e.to_error_string()

    logger.info(f"Inspecting dataset: {dataset_id}")
    output = []

    # Check if CBS dataset exists
    try:
        client = await HTTPClientManager.get_client()
        odata_url = f"{settings.data_base_url}/{dataset_id}"
        response = await client.get(odata_url)
        is_cbs = response.status_code == 200
    except:
        is_cbs = False

    if not is_cbs:
        # Check data.overheid.nl
        ckan_url = f"{settings.ckan_base_url}/package_show?id={dataset_id}"
        try:
            response = await client.get(ckan_url)
            if response.status_code == 200 and response.json().get('success'):
                pkg = response.json()['result']
                output.append(f"DATASET: {dataset_id} (data.overheid.nl - Download only)")
                output.append(f"Title: {pkg.get('title')}")
                desc = (pkg.get('notes') or '')[:200]
                output.append(f"Description: {desc}...")
                output.append(f"Resources: {len(pkg.get('resources', []))} files")
                return "\n".join(output)
        except:
            pass
        return f"Dataset '{dataset_id}' not found in CBS or data.overheid.nl"

    # Get title from catalog
    if not catalog_cache.is_loaded:
        await load_catalog_cache(ctx)
    cbs_match = next((item for item in catalog_cache.data if item.get('Identifier') == dataset_id), None)
    title = cbs_match.get('Title', 'Unknown') if cbs_match else 'Unknown'

    output.append(f"DATASET: {dataset_id}")
    output.append(f"Title: {title}")
    output.append("-" * 50)

    # Fetch structure (DataProperties) - compact format
    try:
        prop_url = f"{settings.data_base_url}/{dataset_id}/DataProperties?$format=json"
        resp = await client.get(prop_url)
        if resp.status_code == 200:
            props = resp.json().get('value', [])
            dimensions = [p for p in props if p.get('Type') == 'Dimension']
            topics = [p for p in props if p.get('Type') == 'Topic']

            output.append(f"DIMENSIONS ({len(dimensions)}):")
            for d in dimensions[:10]:  # Limit to 10
                output.append(f"  {d.get('Key')}: {d.get('Title')}")
            if len(dimensions) > 10:
                output.append(f"  ... and {len(dimensions) - 10} more")

            output.append(f"MEASURES ({len(topics)}):")
            for t in topics[:8]:  # Limit to 8
                output.append(f"  {t.get('Key')}: {t.get('Title')}")
            if len(topics) > 8:
                output.append(f"  ... and {len(topics) - 8} more")
    except Exception as e:
        output.append(f"STRUCTURE: Error - {str(e)[:50]}")

    output.append("-" * 50)

    # Fetch sample data (3 rows only)
    try:
        data_url = f"{settings.data_base_url}/{dataset_id}/TypedDataSet?$format=json&$top=3"
        resp = await client.get(data_url)
        if resp.status_code == 200:
            records = resp.json().get('value', [])
            if records:
                df = pd.DataFrame(records)
                # Translate dimension values
                try:
                    df = await translator.translate_dataframe(df, dataset_id)
                except:
                    pass
                # Limit columns shown
                cols = df.columns.tolist()[:8]
                output.append(f"SAMPLE ({len(records)} rows, showing {len(cols)}/{len(df.columns)} cols):")
                output.append(df[cols].to_csv(index=False))
            else:
                output.append("SAMPLE: No data")
    except Exception as e:
        output.append(f"SAMPLE: Error - {str(e)[:50]}")

    return "\n".join(output)
