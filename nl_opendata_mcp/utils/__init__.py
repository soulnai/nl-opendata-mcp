"""Utility modules for nl-opendata-mcp server."""
from .errors import (
    ErrorCategory,
    MCPError,
    DatasetNotFoundError,
    RateLimitError,
    ValidationError,
    PathTraversalError,
    handle_http_error,
)
from .security import (
    sanitize_odata_filter,
    sanitize_column_name,
    sanitize_select_columns,
    safe_join_path,
    ensure_directory_exists,
    validate_dataset_id,
)

__all__ = [
    # Errors
    "ErrorCategory",
    "MCPError",
    "DatasetNotFoundError",
    "RateLimitError",
    "ValidationError",
    "PathTraversalError",
    "handle_http_error",
    # Security
    "sanitize_odata_filter",
    "sanitize_column_name",
    "sanitize_select_columns",
    "safe_join_path",
    "ensure_directory_exists",
    "validate_dataset_id",
]
