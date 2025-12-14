"""
Discovery tools for finding and listing CBS datasets.
"""
import logging
import pandas as pd
from fastmcp import Context

from ..config import get_settings
from ..models import ListDatasetsInput, SearchDatasetsInput, SearchField, DatasetIdInput
from ..services.cache import catalog_cache
from ..services.http_client import HTTPClientManager, fetch_with_retry
from ..utils import handle_http_error, validate_dataset_id, ValidationError
from .base import load_catalog_cache

logger = logging.getLogger(__name__)
settings = get_settings()


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
    if not catalog_cache.is_loaded:
        await load_catalog_cache(ctx)

    if catalog_cache.data:
        ctx.info(f"Listing datasets from cache (skip={params.skip}, top={params.top})")
        logger.info(f"Listing datasets: skip={params.skip}, top={params.top}")
        data = catalog_cache.data[params.skip : params.skip + params.top]
        if not data:
            return "No datasets found."
        df = pd.DataFrame(data)
        return df.to_csv(index=False)

    # Fallback to API if cache failed
    url = f"{settings.catalog_base_url}/Tables?$format=json&$top={params.top}&$skip={params.skip}"
    ctx.info(f"Listing datasets: {url}")
    logger.info(f"Fetching datasets from API: {url}")
    try:
        response = await fetch_with_retry(url)
        data = response.json().get('value', [])
        if not data:
            return "No datasets found."
        df = pd.DataFrame(data)
        return df.to_csv(index=False)
    except Exception as e:
        return handle_http_error(e, "cbs_list_datasets")


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
    if not catalog_cache.is_loaded:
        await load_catalog_cache(ctx)

    logger.info(f"Searching datasets: query='{params.query}', field={params.search_field}")

    if catalog_cache.data:
        ctx.info(f"Searching datasets in cache for '{params.query}' in {params.search_field}")
        query_lower = params.query.lower()
        matches = []
        for item in catalog_cache.data:
            title = item.get('Title', '').lower()
            summary = item.get('Summary', '').lower() if item.get('Summary') else ''

            if params.search_field == SearchField.TITLE:
                match = query_lower in title
            elif params.search_field == SearchField.SUMMARY:
                match = query_lower in summary
            else:
                match = query_lower in title or query_lower in summary

            if match:
                matches.append(item)

        data = matches[params.skip : params.skip + params.top]
        if not data:
            return "No matching datasets found."
        df = pd.DataFrame(data)
        return df.to_csv(index=False)

    # Fallback to API
    if params.search_field == SearchField.TITLE:
        filter_query = f"substringof('{params.query}', Title)"
    elif params.search_field == SearchField.SUMMARY:
        filter_query = f"substringof('{params.query}', Summary)"
    else:
        filter_query = f"substringof('{params.query}', Title) or substringof('{params.query}', Summary)"

    url = f"{settings.catalog_base_url}/Tables?$format=json&$filter={filter_query}&$top={params.top}&$skip={params.skip}"
    ctx.info(f"Searching datasets with query '{params.query}': {url}")
    try:
        response = await fetch_with_retry(url)
        data = response.json().get('value', [])
        if not data:
            return "No matching datasets found."
        df = pd.DataFrame(data)
        return df.to_csv(index=False)
    except Exception as e:
        return handle_http_error(e, "cbs_search_datasets")


async def cbs_check_dataset_availability(ctx: Context, params: DatasetIdInput) -> str:
    """
    Checks if a dataset is available via CBS OData (queryable) or data.overheid.nl (download-only).

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '83583NED' or 'groningen-parkeervakken')

    Returns:
        str: Availability status and source information
    """
    try:
        dataset_id = validate_dataset_id(params.dataset_id)
    except ValidationError as e:
        return e.to_error_string()

    logger.info(f"Checking dataset availability: {dataset_id}")

    if not catalog_cache.is_loaded:
        await load_catalog_cache(ctx)

    cbs_match = next((item for item in catalog_cache.data if item.get('Identifier') == dataset_id), None)
    if cbs_match:
        return f"Dataset '{dataset_id}' ({cbs_match.get('Title')}) is available and queryable via CBS OData."

    try:
        client = await HTTPClientManager.get_client()
        url = f"{settings.data_base_url}/{dataset_id}"
        response = await client.get(url)
        if response.status_code == 200:
            return f"Dataset '{dataset_id}' is available and queryable via CBS OData (found via direct API check)."
    except:
        pass

    # Check data.overheid.nl (CKAN API)
    ckan_url = f"{settings.ckan_base_url}/package_show?id={dataset_id}"
    ctx.info(f"Checking data.overheid.nl for {dataset_id}...")

    try:
        client = await HTTPClientManager.get_client()
        response = await client.get(ckan_url)
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                resources = data['result'].get('resources', [])
                res_formats = [r.get('format') for r in resources]
                return f"Dataset '{dataset_id}' found on data.overheid.nl. This source is typically download-only and NOT directly queryable. Available formats: {', '.join(res_formats)}."
    except Exception as e:
        ctx.error(f"Error checking data.overheid.nl: {e}")
        logger.error(f"Error checking data.overheid.nl: {e}")

    return f"Dataset '{dataset_id}' was not found in CBS OData catalog or data.overheid.nl."
