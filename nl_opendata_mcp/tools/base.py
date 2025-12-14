"""
Base utilities shared across tool modules.
"""
import logging
from fastmcp import Context

from ..config import get_settings
from ..services.cache import catalog_cache
from ..services.http_client import fetch_with_retry

logger = logging.getLogger(__name__)
settings = get_settings()


async def load_catalog_cache(ctx: Context):
    """Loads the catalog cache from disk or fetches it from the API."""
    # Check if cache is already loaded and not expired
    if catalog_cache.is_loaded and not catalog_cache.is_expired:
        ctx.info(f"Using cached catalog ({len(catalog_cache.data)} datasets, age: {catalog_cache.age_hours:.1f}h)")
        return

    # Try loading from disk
    if not catalog_cache.is_expired and catalog_cache.data:
        ctx.info(f"Loaded {len(catalog_cache.data)} datasets from cache.")
        logger.info(f"Catalog cache loaded: {len(catalog_cache.data)} datasets")
        return

    # Fetch from API
    ctx.info("Fetching full catalog from API (this may take a moment)...")
    url = f"{settings.catalog_base_url}/Tables?$format=json&$top=10000&$select=Identifier,Title,Summary"
    try:
        response = await fetch_with_retry(url)
        data = response.json()
        catalog_cache.data = data.get('value', [])
        ctx.info(f"Cached {len(catalog_cache.data)} datasets (TTL: {catalog_cache.ttl_hours}h).")
        logger.info(f"Catalog fetched and cached: {len(catalog_cache.data)} datasets")
    except Exception as e:
        ctx.error(f"Error fetching catalog: {e}")
        logger.error(f"Failed to fetch catalog: {e}")
