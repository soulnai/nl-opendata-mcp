"""
HTTP client management for nl-opendata-mcp server.

This module provides a shared HTTP client with:
- Connection pooling for improved performance
- Automatic retry with exponential backoff
- HTTP/2 support
- Proper lifecycle management (initialization/cleanup)

The HTTPClientManager implements a singleton pattern to ensure
a single client instance is reused across all requests.

Example:
    >>> from nl_opendata_mcp.services import HTTPClientManager, fetch_with_retry
    >>>
    >>> # Get the shared client
    >>> client = await HTTPClientManager.get_client()
    >>>
    >>> # Or use fetch_with_retry for automatic retries
    >>> response = await fetch_with_retry("https://api.example.com/data")
    >>>
    >>> # Cleanup on shutdown
    >>> await HTTPClientManager.close()
"""
import asyncio
import logging
from typing import Optional, Any
from contextlib import asynccontextmanager

import httpx

from ..config import get_settings

logger = logging.getLogger(__name__)


class HTTPClientManager:
    """
    Manages a shared HTTP client with connection pooling.

    This class implements the singleton pattern to ensure a single
    HTTP client is reused across all requests, improving performance
    through connection pooling.
    """

    _client: Optional[httpx.AsyncClient] = None
    _lock: Optional[asyncio.Lock] = None
    _loop_id: Optional[int] = None

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        """Get or create a lock for the current event loop."""
        try:
            current_loop = asyncio.get_running_loop()
            current_loop_id = id(current_loop)
        except RuntimeError:
            current_loop_id = None

        # Create new lock if none exists or if event loop changed
        if cls._lock is None or cls._loop_id != current_loop_id:
            cls._lock = asyncio.Lock()
            cls._loop_id = current_loop_id

        return cls._lock

    @classmethod
    async def get_client(cls) -> httpx.AsyncClient:
        """
        Get or create the shared HTTP client.

        Returns:
            Configured httpx.AsyncClient instance
        """
        # Check if client exists and is still usable in current event loop
        if cls._client is not None:
            try:
                # Test if the client is still usable
                current_loop = asyncio.get_running_loop()
                # If loop changed, close old client and create new one
                if cls._loop_id != id(current_loop):
                    try:
                        await cls._client.aclose()
                    except Exception:
                        pass
                    cls._client = None
            except Exception:
                cls._client = None

        if cls._client is None:
            lock = cls._get_lock()
            async with lock:
                # Double-check pattern
                if cls._client is None:
                    settings = get_settings()
                    cls._client = httpx.AsyncClient(
                        timeout=httpx.Timeout(
                            settings.http_timeout,
                            connect=settings.connect_timeout
                        ),
                        limits=httpx.Limits(
                            max_connections=settings.max_connections,
                            max_keepalive_connections=settings.max_keepalive_connections
                        ),
                        follow_redirects=True,
                        http2=True
                    )
                    try:
                        cls._loop_id = id(asyncio.get_running_loop())
                    except RuntimeError:
                        pass
                    logger.info("HTTP client initialized with connection pooling")
        return cls._client

    @classmethod
    async def close(cls) -> None:
        """Close the shared HTTP client and release resources."""
        lock = cls._get_lock()
        async with lock:
            if cls._client is not None:
                try:
                    await cls._client.aclose()
                except Exception:
                    pass
                cls._client = None
                logger.info("HTTP client closed")

    @classmethod
    def is_initialized(cls) -> bool:
        """Check if client is initialized."""
        return cls._client is not None


async def fetch_with_retry(
    url: str,
    max_retries: Optional[int] = None,
    retry_on_status: Optional[set[int]] = None
) -> httpx.Response:
    """
    Fetch URL with automatic retry on failure.

    Implements exponential backoff for transient failures.

    Args:
        url: URL to fetch
        max_retries: Maximum retry attempts (default from settings)
        retry_on_status: HTTP status codes to retry on (default: 429, 500, 502, 503, 504)

    Returns:
        HTTP response

    Raises:
        httpx.HTTPStatusError: If request fails after all retries
        httpx.RequestError: If request fails due to network error after all retries
    """
    settings = get_settings()
    max_retries = max_retries if max_retries is not None else settings.max_retries
    retry_on_status = retry_on_status or {429, 500, 502, 503, 504}

    client = await HTTPClientManager.get_client()
    last_exception: Optional[Exception] = None

    for attempt in range(max_retries + 1):
        try:
            response = await client.get(url)

            # Check if we should retry based on status code
            if response.status_code in retry_on_status and attempt < max_retries:
                wait_time = _calculate_backoff(attempt, settings, response)
                logger.warning(
                    f"Request to {url} returned {response.status_code}, "
                    f"retrying in {wait_time:.1f}s (attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(wait_time)
                continue

            # Raise for other error status codes
            response.raise_for_status()
            return response

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            last_exception = e
            if attempt < max_retries:
                wait_time = _calculate_backoff(attempt, settings)
                logger.warning(
                    f"Request to {url} failed with {type(e).__name__}, "
                    f"retrying in {wait_time:.1f}s (attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(wait_time)
            else:
                raise

    # Should not reach here, but just in case
    if last_exception:
        raise last_exception
    raise httpx.RequestError(f"Request failed after {max_retries} retries")


def _calculate_backoff(
    attempt: int,
    settings: Any,
    response: Optional[httpx.Response] = None
) -> float:
    """
    Calculate backoff time for retry.

    Uses exponential backoff with jitter, respecting Retry-After header if present.

    Args:
        attempt: Current attempt number (0-based)
        settings: Application settings
        response: Optional HTTP response (for Retry-After header)

    Returns:
        Wait time in seconds
    """
    # Check for Retry-After header
    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(float(retry_after), settings.retry_max_wait)
            except ValueError:
                pass

    # Exponential backoff: min_wait * 2^attempt
    backoff = settings.retry_min_wait * (2 ** attempt)

    # Add jitter (up to 25% of backoff time)
    import random
    jitter = backoff * 0.25 * random.random()

    return min(backoff + jitter, settings.retry_max_wait)


@asynccontextmanager
async def get_http_client():
    """
    Context manager for HTTP client access.

    This is a convenience wrapper that doesn't close the client,
    as the client is managed globally.

    Usage:
        async with get_http_client() as client:
            response = await client.get(url)
    """
    client = await HTTPClientManager.get_client()
    yield client


async def fetch_json(url: str, default: Any = None) -> Any:
    """
    Fetch JSON from URL with retry logic.

    Args:
        url: URL to fetch
        default: Default value if fetch fails

    Returns:
        Parsed JSON response or default value
    """
    try:
        response = await fetch_with_retry(url)
        return response.json()
    except Exception as e:
        logger.error(f"Failed to fetch JSON from {url}: {e}")
        if default is not None:
            return default
        raise


async def check_url_reachable(url: str, timeout: float = 5.0) -> bool:
    """
    Check if URL is reachable.

    Args:
        url: URL to check
        timeout: Timeout in seconds

    Returns:
        True if URL is reachable, False otherwise
    """
    try:
        client = await HTTPClientManager.get_client()
        response = await client.head(url, timeout=timeout)
        return response.status_code < 500
    except Exception:
        return False
