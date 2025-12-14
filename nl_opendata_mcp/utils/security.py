"""
Security utilities for nl-opendata-mcp server.

This module provides security and validation utilities:
    - sanitize_odata_filter: Prevents OData injection attacks
    - sanitize_select_columns: Validates column names
    - safe_join_path: Prevents path traversal attacks
    - validate_dataset_id: Validates CBS dataset identifiers
"""
import os
import re
import logging
from typing import Optional

from .errors import ValidationError, PathTraversalError

logger = logging.getLogger(__name__)

# Allowed OData operators and functions
ODATA_OPERATORS = frozenset([
    'eq', 'ne', 'gt', 'lt', 'ge', 'le',
    'and', 'or', 'not',
    'add', 'sub', 'mul', 'div', 'mod'
])

ODATA_FUNCTIONS = frozenset([
    'substringof', 'startswith', 'endswith',
    'length', 'indexof', 'replace', 'substring',
    'tolower', 'toupper', 'trim', 'concat',
    'year', 'month', 'day', 'hour', 'minute', 'second',
    'round', 'floor', 'ceiling'
])

# Pattern for potentially dangerous characters
DANGEROUS_PATTERN = re.compile(r'[;<>{}|\\\x00-\x1f]')

# Pattern for valid OData identifiers
IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def sanitize_odata_filter(filter_str: Optional[str]) -> Optional[str]:
    """
    Sanitize OData filter string to prevent injection attacks.

    Args:
        filter_str: The OData filter string to sanitize

    Returns:
        Sanitized filter string, or None if input was None

    Raises:
        ValidationError: If filter contains dangerous patterns
    """
    if filter_str is None:
        return None

    filter_str = filter_str.strip()
    if not filter_str:
        return None

    # Check for dangerous characters
    if DANGEROUS_PATTERN.search(filter_str):
        raise ValidationError(
            "Filter contains invalid characters",
            field="filter"
        )

    # Check for excessively long filters (potential DoS)
    if len(filter_str) > 2000:
        raise ValidationError(
            "Filter is too long (max 2000 characters)",
            field="filter"
        )

    # Check for balanced parentheses
    paren_count = 0
    for char in filter_str:
        if char == '(':
            paren_count += 1
        elif char == ')':
            paren_count -= 1
        if paren_count < 0:
            raise ValidationError(
                "Filter has unbalanced parentheses",
                field="filter"
            )
    if paren_count != 0:
        raise ValidationError(
            "Filter has unbalanced parentheses",
            field="filter"
        )

    # Check for balanced quotes
    single_quotes = filter_str.count("'")
    if single_quotes % 2 != 0:
        raise ValidationError(
            "Filter has unbalanced quotes",
            field="filter"
        )

    logger.debug(f"Sanitized OData filter: {filter_str}")
    return filter_str


def sanitize_column_name(name: str) -> str:
    """
    Sanitize column name for OData select.

    Args:
        name: Column name to sanitize

    Returns:
        Sanitized column name

    Raises:
        ValidationError: If column name is invalid
    """
    name = name.strip()
    if not name:
        raise ValidationError("Column name cannot be empty", field="select")

    if not IDENTIFIER_PATTERN.match(name):
        raise ValidationError(
            f"Invalid column name: '{name}'. Must start with letter/underscore and contain only alphanumeric characters.",
            field="select"
        )

    if len(name) > 128:
        raise ValidationError(
            f"Column name too long: '{name}' (max 128 characters)",
            field="select"
        )

    return name


def sanitize_select_columns(columns: Optional[list[str]]) -> Optional[list[str]]:
    """
    Sanitize list of column names for OData select.

    Args:
        columns: List of column names to sanitize

    Returns:
        List of sanitized column names, or None if input was None
    """
    if columns is None:
        return None

    return [sanitize_column_name(col) for col in columns]


def safe_join_path(base_dir: str, filename: str) -> str:
    """
    Safely join base directory and filename, preventing path traversal.

    Args:
        base_dir: The base directory (must be absolute or will be made absolute)
        filename: The filename to join (will be sanitized)

    Returns:
        Absolute path within base directory

    Raises:
        PathTraversalError: If resulting path would be outside base directory
        ValidationError: If filename is invalid
    """
    if not filename:
        raise ValidationError("Filename cannot be empty", field="file_name")

    # Normalize base directory to absolute path
    base = os.path.abspath(base_dir)

    # Remove any path components from filename (keep only the basename)
    # This prevents ../../../etc/passwd style attacks
    safe_filename = os.path.basename(filename)

    if not safe_filename:
        raise ValidationError(
            "Invalid filename after sanitization",
            field="file_name"
        )

    # Additional checks on filename
    if safe_filename.startswith('.'):
        raise ValidationError(
            "Filename cannot start with a dot",
            field="file_name"
        )

    # Check for null bytes
    if '\x00' in safe_filename:
        raise ValidationError(
            "Filename contains invalid characters",
            field="file_name"
        )

    # Build and verify the full path
    full_path = os.path.abspath(os.path.join(base, safe_filename))

    # Verify the path is within base directory
    if not full_path.startswith(base + os.sep) and full_path != base:
        raise PathTraversalError(filename)

    return full_path


def ensure_directory_exists(path: str) -> None:
    """
    Ensure directory exists, creating it if necessary.

    Args:
        path: Directory path to ensure exists
    """
    os.makedirs(path, exist_ok=True)


def validate_dataset_id(dataset_id: str) -> str:
    """
    Validate CBS dataset ID format.

    Args:
        dataset_id: Dataset ID to validate

    Returns:
        Validated dataset ID (trimmed)

    Raises:
        ValidationError: If dataset ID is invalid
    """
    dataset_id = dataset_id.strip()

    if not dataset_id:
        raise ValidationError("Dataset ID cannot be empty", field="dataset_id")

    if len(dataset_id) > 100:
        raise ValidationError(
            "Dataset ID too long (max 100 characters)",
            field="dataset_id"
        )

    # CBS dataset IDs typically match pattern like "85313NED" or "83583NED"
    # But data.overheid.nl IDs can be slugs like "groningen-parkeervakken"
    # Allow alphanumeric, hyphens, and underscores
    if not re.match(r'^[a-zA-Z0-9_-]+$', dataset_id):
        raise ValidationError(
            "Dataset ID contains invalid characters. Use only alphanumeric characters, hyphens, and underscores.",
            field="dataset_id"
        )

    return dataset_id
