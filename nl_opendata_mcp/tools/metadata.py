"""
Metadata tools for retrieving CBS dataset information and structure.
"""
import json
import logging
import pandas as pd
from fastmcp import Context

from ..config import get_settings
from ..models import DatasetIdInput, QueryMetadataInput, GetMetadataInput, MetadataType
from ..services.http_client import fetch_with_retry
from ..utils import handle_http_error, validate_dataset_id, ValidationError

logger = logging.getLogger(__name__)
settings = get_settings()


async def cbs_get_dataset_info(ctx: Context, params: DatasetIdInput) -> str:
    """
    Gets detailed information and description for a specific dataset (TableInfos).

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')

    Returns:
        str: CSV string containing dataset metadata
    """
    try:
        dataset_id = validate_dataset_id(params.dataset_id)
    except ValidationError as e:
        return e.to_error_string()

    url = f"{settings.data_base_url}/{dataset_id}/TableInfos?$format=json"
    ctx.info(f"Getting info for dataset {dataset_id}: {url}")
    logger.info(f"Getting dataset info: {dataset_id}")

    try:
        response = await fetch_with_retry(url)
        data = response.json().get('value', [])
        if not data:
            return "No info found."
        df = pd.DataFrame(data)
        return df.to_csv(index=False)
    except Exception as e:
        return handle_http_error(e, "cbs_get_dataset_info")


async def cbs_get_table_structure(ctx: Context, params: DatasetIdInput) -> str:
    """
    Gets the structure (columns and data types) of a dataset (DataProperties).

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')

    Returns:
        str: CSV string containing column definitions (Key, Type, Title, Description)
    """
    try:
        dataset_id = validate_dataset_id(params.dataset_id)
    except ValidationError as e:
        return e.to_error_string()

    url = f"{settings.data_base_url}/{dataset_id}/DataProperties?$format=json"
    ctx.info(f"Getting structure for dataset {dataset_id}: {url}")
    logger.info(f"Getting table structure: {dataset_id}")

    try:
        response = await fetch_with_retry(url)
        data = response.json().get('value', [])
        if not data:
            return "No table structure found."
        df = pd.DataFrame(data)
        return df.to_csv(index=False)
    except Exception as e:
        return handle_http_error(e, "cbs_get_table_structure")


async def cbs_get_dataset_metadata(ctx: Context, params: DatasetIdInput) -> str:
    """
    Gets metadata and classification links for a dataset.

    Args:
        params: DatasetIdInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')

    Returns:
        str: JSON string containing dataset metadata and available endpoints
    """
    try:
        dataset_id = validate_dataset_id(params.dataset_id)
    except ValidationError as e:
        return e.to_error_string()

    url = f"{settings.data_base_url}/{dataset_id}"
    ctx.info(f"Getting metadata for dataset {dataset_id}: {url}")
    logger.info(f"Getting dataset metadata: {dataset_id}")

    try:
        response = await fetch_with_retry(url)
        data = response.json()
        return json.dumps(data, indent=2)
    except Exception as e:
        return handle_http_error(e, "cbs_get_dataset_metadata")


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
    try:
        dataset_id = validate_dataset_id(params.dataset_id)
    except ValidationError as e:
        return e.to_error_string()

    url = f"{settings.data_base_url}/{dataset_id}/{params.metadata_name}"
    ctx.info(f"Querying metadata for dataset {dataset_id}: {url}")
    logger.info(f"Querying dataset metadata: {dataset_id}/{params.metadata_name}")

    try:
        response = await fetch_with_retry(url)
        data = response.json()
        return json.dumps(data, indent=2)
    except Exception as e:
        return handle_http_error(e, "cbs_query_dataset_metadata")


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
    try:
        dataset_id = validate_dataset_id(params.dataset_id)
    except ValidationError as e:
        return e.to_error_string()

    logger.info(f"Getting metadata: dataset={dataset_id}, type={params.metadata_type}")

    # Build URL based on metadata type
    if params.metadata_type == MetadataType.INFO:
        url = f"{settings.data_base_url}/{dataset_id}/TableInfos?$format=json"
        return_csv = True
    elif params.metadata_type == MetadataType.STRUCTURE:
        url = f"{settings.data_base_url}/{dataset_id}/DataProperties?$format=json"
        return_csv = True
    elif params.metadata_type == MetadataType.ENDPOINTS:
        url = f"{settings.data_base_url}/{dataset_id}"
        return_csv = False
    elif params.metadata_type == MetadataType.CUSTOM:
        if not params.custom_endpoint:
            return "Error: custom_endpoint is required when metadata_type='custom'"
        url = f"{settings.data_base_url}/{dataset_id}/{params.custom_endpoint}"
        return_csv = False
    else:
        return f"Error: Unknown metadata type: {params.metadata_type}"

    ctx.info(f"Fetching metadata from: {url}")

    try:
        response = await fetch_with_retry(url)
        data = response.json()

        if return_csv:
            records = data.get('value', [])
            if not records:
                return "No metadata found."
            df = pd.DataFrame(records)
            return df.to_csv(index=False)
        else:
            return json.dumps(data, indent=2)

    except Exception as e:
        return handle_http_error(e, "cbs_get_metadata")
