"""MCP tool implementations for nl-opendata-mcp server."""
from .discovery import (
    cbs_list_datasets,
    cbs_search_datasets,
    cbs_check_dataset_availability,
)
from .metadata import (
    cbs_get_metadata,
)
from .query import (
    cbs_query_dataset,
    cbs_estimate_dataset_size,
    cbs_inspect_dataset_details,
)
from .export import (
    cbs_save_dataset,
    cbs_save_dataset_to_duckdb,
)
from .analysis import (
    cbs_analyze_remote_dataset,
    cbs_analyze_local_dataset,
    cbs_list_local_datasets,
)

__all__ = [
    # Discovery
    "cbs_list_datasets",
    "cbs_search_datasets",
    "cbs_check_dataset_availability",
    # Metadata
    "cbs_get_metadata",
    # Query
    "cbs_query_dataset",
    "cbs_estimate_dataset_size",
    "cbs_inspect_dataset_details",
    # Export
    "cbs_save_dataset",
    "cbs_save_dataset_to_duckdb",
    # Analysis
    "cbs_analyze_remote_dataset",
    "cbs_analyze_local_dataset",
    "cbs_list_local_datasets",
]
