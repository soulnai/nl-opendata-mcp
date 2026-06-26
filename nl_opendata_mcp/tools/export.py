"""
Export tools for saving CBS datasets to files.
"""
import logging
import pandas as pd
from fastmcp import Context

from ..config import get_settings
from ..models import SaveDatasetInput
from ..services.cache import dataset_cache
from ..services.http_client import HTTPClientManager
from ..services.translator import translator
from ..utils import (
    handle_http_error,
    validate_dataset_id,
    safe_join_path,
    ensure_directory_exists,
    ValidationError,
    MCPError,
)

logger = logging.getLogger(__name__)
settings = get_settings()


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
    try:
        dataset_id = validate_dataset_id(params.dataset_id)
        # Use safe path joining to prevent path traversal
        ensure_directory_exists(settings.downloads_path)
        full_path = safe_join_path(settings.downloads_path, params.file_name)
    except (ValidationError, MCPError) as e:
        return e.to_error_string()

    logger.info(f"Saving dataset {dataset_id} to {full_path}")

    try:
        # Check cache
        if dataset_cache.exists(full_path):
            ctx.info(f"Serving dataset from cache: {full_path}")
            return f"Dataset already saved to {full_path} (cached)"

        client = await HTTPClientManager.get_client()

        if params.fetch_all:
            ctx.info(f"Fetching full dataset {dataset_id} with pagination...")
            all_records = []
            batch_size = settings.batch_size
            current_skip = 0

            while True:
                url = f"{settings.data_base_url}/{dataset_id}/TypedDataSet?$format=json&$top={batch_size}&$skip={current_skip}"
                ctx.info(f"Fetching batch: skip={current_skip}, top={batch_size}")

                response = await client.get(url)
                response.raise_for_status()
                records = response.json().get('value', [])

                if not records:
                    break

                all_records.extend(records)
                current_skip += batch_size

                if current_skip > settings.max_records_per_fetch:
                    ctx.warning(f"Reached maximum record limit ({settings.max_records_per_fetch:,}). Stopping pagination.")
                    break

            if not all_records:
                return "No data found in dataset."

            df = pd.DataFrame(all_records)

            # Apply translation if requested (values only, column names stay as valid identifiers)
            if params.translate:
                try:
                    ctx.info(f"Translating dimension values for {dataset_id}...")
                    df = await translator.translate_dataframe(df, dataset_id)
                except Exception as e:
                    logger.warning(f"Translation failed for {dataset_id}: {e}")
                    ctx.warning(f"Translation failed, saving with original codes: {e}")

            df.to_csv(full_path, index=False)

            # Update cache
            dataset_cache.set(full_path, dataset_id, len(all_records))

            translated_msg = " (translated)" if params.translate else ""
            logger.info(f"Dataset saved: {full_path} ({len(all_records)} records){translated_msg}")
            return f"Full dataset saved to {full_path} ({len(all_records)} records){translated_msg}"
        else:
            url = f"{settings.data_base_url}/{dataset_id}/TypedDataSet?$format=json&$top={params.top}&$skip={params.skip}"
            ctx.info(f"Fetching data for dataset: {url}")

            response = await client.get(url)
            response.raise_for_status()
            records = response.json().get('value', [])

            if not records:
                return "No data found in dataset."

            df = pd.DataFrame(records)

            # Apply translation if requested (values only, column names stay as valid identifiers)
            if params.translate:
                try:
                    ctx.info(f"Translating dimension values for {dataset_id}...")
                    df = await translator.translate_dataframe(df, dataset_id)
                except Exception as e:
                    logger.warning(f"Translation failed for {dataset_id}: {e}")
                    ctx.warning(f"Translation failed, saving with original codes: {e}")

            df.to_csv(full_path, index=False)

            # Update cache
            dataset_cache.set(full_path, dataset_id, len(records))

            translated_msg = " (translated)" if params.translate else ""
            logger.info(f"Dataset saved: {full_path} ({len(records)} records){translated_msg}")
            return f"Dataset saved to {full_path} ({len(records)} records){translated_msg}"

    except Exception as e:
        return handle_http_error(e, "cbs_save_dataset")
