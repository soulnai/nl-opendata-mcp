"""
Error handling utilities for nl-opendata-mcp server.

This module provides structured error handling:
    - MCPError: Base exception class with categorization
    - ValidationError: Input validation failures
    - DatasetNotFoundError: Dataset not found errors
    - RateLimitError: API rate limit errors
    - handle_http_error: Convert HTTP errors to user-friendly messages
"""
import logging
from enum import Enum
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


class ErrorCategory(str, Enum):
    """Error classification for structured error handling."""
    CLIENT = "client_error"
    SERVER = "server_error"
    NETWORK = "network_error"
    VALIDATION = "validation_error"
    EXTERNAL = "external_error"


class MCPError(Exception):
    """Base exception for MCP server errors."""

    def __init__(
        self,
        message: str,
        category: ErrorCategory,
        retry_after: Optional[int] = None,
        details: Optional[dict] = None
    ):
        self.message = message
        self.category = category
        self.retry_after = retry_after
        self.details = details or {}
        super().__init__(message)

    @property
    def is_retryable(self) -> bool:
        """Check if error is retryable."""
        return self.category in [ErrorCategory.NETWORK, ErrorCategory.SERVER]

    def to_error_string(self) -> str:
        """Convert to user-friendly error string."""
        base = f"Error: {self.message}"
        if self.retry_after:
            base += f" (retry after {self.retry_after}s)"
        if self.details:
            detail_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            base += f" [{detail_str}]"
        return base


class DatasetNotFoundError(MCPError):
    """Raised when a dataset is not found."""

    def __init__(self, dataset_id: str):
        super().__init__(
            f"Dataset '{dataset_id}' not found. Please check the dataset ID is correct.",
            ErrorCategory.CLIENT,
            details={"dataset_id": dataset_id}
        )


class RateLimitError(MCPError):
    """Raised when API rate limit is exceeded."""

    def __init__(self, retry_after: int = 60):
        super().__init__(
            "Rate limit exceeded. Please wait before making more requests.",
            ErrorCategory.EXTERNAL,
            retry_after=retry_after
        )


class ValidationError(MCPError):
    """Raised for input validation failures."""

    def __init__(self, message: str, field: Optional[str] = None):
        super().__init__(
            message,
            ErrorCategory.VALIDATION,
            details={"field": field} if field else {}
        )


class PathTraversalError(MCPError):
    """Raised when path traversal is detected."""

    def __init__(self, path: str):
        super().__init__(
            "Invalid file path: path traversal detected",
            ErrorCategory.VALIDATION,
            details={"path": path}
        )


def handle_http_error(e: Exception, context: str = "") -> str:
    """
    Convert HTTP exceptions to user-friendly error strings.

    Args:
        e: The exception to handle
        context: Optional context string for logging

    Returns:
        User-friendly error message string
    """
    if context:
        logger.error(f"{context}: {e}")

    if isinstance(e, MCPError):
        return e.to_error_string()

    if isinstance(e, httpx.HTTPStatusError):
        status = e.response.status_code

        # Try to extract actual error message from response body
        response_detail = ""
        try:
            # Try JSON response first (common for APIs)
            response_json = e.response.json()
            if isinstance(response_json, dict):
                # Common error message field names
                for key in ("error", "message", "detail", "error_description", "odata.error"):
                    if key in response_json:
                        error_val = response_json[key]
                        if isinstance(error_val, dict):
                            response_detail = error_val.get("message", str(error_val))
                        else:
                            response_detail = str(error_val)
                        break
                if not response_detail:
                    response_detail = str(response_json)
        except Exception:
            # Fall back to text response
            try:
                text = e.response.text.strip()
                if text and len(text) < 500:  # Only include if reasonable length
                    response_detail = text
            except Exception:
                pass

        detail_suffix = f" Details: {response_detail}" if response_detail else ""

        if status == 404:
            return f"Error: Resource not found. Please check the dataset ID is correct.{detail_suffix}"
        elif status == 403:
            return f"Error: Permission denied. Access to this resource is restricted.{detail_suffix}"
        elif status == 429:
            retry_after = int(e.response.headers.get("Retry-After", 60))
            return f"Error: Rate limit exceeded. Please wait {retry_after}s before making more requests.{detail_suffix}"
        elif 500 <= status < 600:
            return f"Error: CBS API server error (status {status}). Please try again later.{detail_suffix}"
        return f"Error: API request failed with status {status}.{detail_suffix}"

    elif isinstance(e, httpx.TimeoutException):
        return "Error: Request timed out. The CBS API may be slow. Please try again."

    elif isinstance(e, httpx.ConnectError):
        return "Error: Could not connect to CBS API. Please check your internet connection."

    elif isinstance(e, httpx.RequestError):
        return f"Error: Network error: {str(e)}"

    return f"Error: Unexpected error: {type(e).__name__}: {str(e)}"
