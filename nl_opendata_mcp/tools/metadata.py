"""
Metadata tools for retrieving CBS dataset information and structure.
"""
import json
import logging
import pandas as pd
from fastmcp import Context

from ..config import get_settings
from ..models import GetMetadataInput, MetadataType
from ..services.http_client import fetch_with_retry
from ..utils import handle_http_error, validate_dataset_id, ValidationError

logger = logging.getLogger(__name__)
settings = get_settings()


async def cbs_get_metadata(ctx: Context, params: GetMetadataInput) -> str:
    """
    Unified tool to get dataset metadata. Handles all metadata retrieval needs.

    Args:
        params: GetMetadataInput containing:
            - dataset_id (str): Dataset ID (e.g., '85313NED')
            - metadata_type (str): Type of metadata:
                - 'info': Dataset description and details (TableInfos)
                - 'structure': Column definitions and types (DataProperties)
                - 'endpoints': Available metadata endpoints
                - 'dimensions': Dimension values with codes for filtering (requires endpoint_name)
                - 'custom': Custom endpoint query (requires endpoint_name)
            - endpoint_name (str, optional): Required for 'dimensions' and 'custom' types
              (e.g., 'Geslacht', 'Perioden', 'Luchthavens')

    Returns:
        str: CSV for info/structure/dimensions, JSON for endpoints/custom

    Examples:
        - Get dataset info: metadata_type="info"
        - Get columns: metadata_type="structure"
        - Get dimension codes: metadata_type="dimensions", endpoint_name="Geslacht"
        - Get raw endpoint: metadata_type="custom", endpoint_name="CategoryGroups"

    IMPORTANT - Dimension Values:
        Use metadata_type="dimensions" to find codes for OData filtering.
        CBS datasets use coded values (e.g., 'A043591') that map to
        human-readable names (e.g., 'Eindhoven Airport').

        Example workflow:
        1. Get dimensions: metadata_type="dimensions", endpoint_name="Luchthavens"
        2. Use code in filter: filter="Luchthavens eq 'A043591'"
    """
    try:
        dataset_id = validate_dataset_id(params.dataset_id)
    except ValidationError as e:
        return e.to_error_string()

    logger.info(f"Getting metadata: dataset={dataset_id}, type={params.metadata_type}")

    # Build URL and determine output format based on metadata type
    if params.metadata_type == MetadataType.INFO:
        url = f"{settings.data_base_url}/{dataset_id}/TableInfos?$format=json"
        return await _fetch_csv_metadata(ctx, url, "info")

    elif params.metadata_type == MetadataType.STRUCTURE:
        url = f"{settings.data_base_url}/{dataset_id}/DataProperties?$format=json"
        return await _fetch_csv_metadata(ctx, url, "structure")

    elif params.metadata_type == MetadataType.ENDPOINTS:
        url = f"{settings.data_base_url}/{dataset_id}"
        return await _fetch_json_metadata(ctx, url)

    elif params.metadata_type == MetadataType.DIMENSIONS:
        if not params.endpoint_name:
            return "Error: endpoint_name is required when metadata_type='dimensions' (e.g., 'Geslacht', 'Perioden')"
        return await _fetch_dimension_values(ctx, dataset_id, params.endpoint_name)

    elif params.metadata_type == MetadataType.CUSTOM:
        if not params.endpoint_name:
            return "Error: endpoint_name is required when metadata_type='custom'"
        url = f"{settings.data_base_url}/{dataset_id}/{params.endpoint_name}"
        return await _fetch_json_metadata(ctx, url)

    else:
        return f"Error: Unknown metadata type: {params.metadata_type}"


async def _fetch_csv_metadata(ctx: Context, url: str, metadata_type: str) -> str:
    """Fetch metadata and return as CSV."""
    ctx.info(f"Fetching {metadata_type} metadata from: {url}")
    try:
        response = await fetch_with_retry(url)
        data = response.json()
        records = data.get('value', [])
        if not records:
            return f"No {metadata_type} metadata found."
        df = pd.DataFrame(records)
        return df.to_csv(index=False)
    except Exception as e:
        return handle_http_error(e, "cbs_get_metadata")


async def _fetch_json_metadata(ctx: Context, url: str) -> str:
    """Fetch metadata and return as JSON."""
    ctx.info(f"Fetching metadata from: {url}")
    try:
        response = await fetch_with_retry(url)
        data = response.json()
        return json.dumps(data, indent=2)
    except Exception as e:
        return handle_http_error(e, "cbs_get_metadata")


async def _fetch_dimension_values(ctx: Context, dataset_id: str, dimension_name: str) -> str:
    """
    Fetch dimension values with codes for OData filtering.
    Returns a formatted table with Code, Title, Description.
    """
    dimension_name = dimension_name.strip()
    url = f"{settings.data_base_url}/{dataset_id}/{dimension_name}?$format=json"
    ctx.info(f"Getting dimension values for {dataset_id}/{dimension_name}")
    logger.info(f"Getting dimension values: {dataset_id}/{dimension_name}")

    try:
        response = await fetch_with_retry(url)
        data = response.json()

        # Handle both direct array and 'value' wrapper
        if isinstance(data, dict):
            records = data.get('value', [])
        else:
            records = data

        if not records:
            return f"No values found for dimension '{dimension_name}'.\n\nTIP: Use metadata_type='structure' to see available dimensions (look for Type='Dimension')."

        # Extract relevant columns
        output_records = []
        for r in records:
            output_records.append({
                'Code': r.get('Key', r.get('Identifier', '')),
                'Title': r.get('Title', ''),
                'Description': (r.get('Description', '') or '')[:80]
            })

        df = pd.DataFrame(output_records)

        # Compact format output
        header = [
            f"DIMENSION: {dimension_name} ({len(df)} values)",
            f"Use Code in filter: {dimension_name} eq '<Code>'",
            "-" * 50,
        ]

        return "\n".join(header) + "\n" + df.to_string(index=False)

    except Exception as e:
        error_msg = str(e)
        if "404" in error_msg or "Not Found" in error_msg:
            return f"Dimension '{dimension_name}' not found.\n\nTIP: Use metadata_type='structure' to see available dimensions."
        return handle_http_error(e, "cbs_get_metadata")
