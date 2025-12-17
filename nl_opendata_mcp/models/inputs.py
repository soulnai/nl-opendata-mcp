"""
Pydantic input models for nl-opendata-mcp tools.

These models provide validation and documentation for tool parameters.
"""
from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum


class ListDatasetsInput(BaseModel):
    """Input model for listing datasets."""
    model_config = ConfigDict(str_strip_whitespace=True)
    top: int = Field(default=10, ge=1, le=1000, description="Number of records to return (1-1000)")
    skip: int = Field(default=0, ge=0, description="Number of records to skip for pagination")


class SearchField(str, Enum):
    """Search field options."""
    ALL = "all"
    TITLE = "title"
    SUMMARY = "summary"


class SearchDatasetsInput(BaseModel):
    """Input model for searching datasets."""
    model_config = ConfigDict(str_strip_whitespace=True)
    query: str = Field(..., min_length=1, description="Search term (e.g., 'Bevolking', 'Inflation')")
    top: int = Field(default=10, ge=1, le=100, description="Number of records to return")
    skip: int = Field(default=0, ge=0, description="Number of records to skip")
    search_field: SearchField = Field(default=SearchField.ALL, description="Where to search: 'all', 'title', or 'summary'")


class DatasetIdInput(BaseModel):
    """Input model for operations that only need a dataset ID."""
    model_config = ConfigDict(str_strip_whitespace=True)
    dataset_id: str = Field(..., min_length=1, description="Dataset ID (e.g., '85313NED')")


class SaveDatasetInput(BaseModel):
    """Input model for saving datasets."""
    model_config = ConfigDict(str_strip_whitespace=True)
    dataset_id: str = Field(..., min_length=1, description="Dataset ID (e.g., '85313NED')")
    file_name: str = Field(..., min_length=1, description="File name to save the dataset")
    top: int = Field(default=1000, ge=1, description="Records per request (only if fetch_all=False)")
    skip: int = Field(default=0, ge=0, description="Records to skip (only if fetch_all=False)")
    fetch_all: bool = Field(default=False, description="If True, fetch all records using pagination")
    translate: bool = Field(default=True, description="Translate coded values to human-readable text (dimension values and column names)")


class OutputFormat(str, Enum):
    """Output format options."""
    CSV = "csv"
    PARQUET = "parquet"


class SaveToDuckDBInput(BaseModel):
    """Input model for saving to DuckDB."""
    model_config = ConfigDict(str_strip_whitespace=True)
    dataset_id: str = Field(..., min_length=1, description="Dataset ID (e.g., '85313NED')")
    table_name: Optional[str] = Field(default=None, description="Table name (default: dataset_id)")
    fetch_all: bool = Field(default=True, description="Fetch all records using pagination")
    select: Optional[List[str]] = Field(default=None, description="Column names to fetch")


class AnalyzeRemoteInput(BaseModel):
    """Input model for remote dataset analysis."""
    model_config = ConfigDict(str_strip_whitespace=True)
    dataset_id: str = Field(..., min_length=1, description="Dataset ID (e.g., '85313NED')")
    analysis_code: Optional[str] = Field(default=None, description="Python code to execute on 'df' DataFrame (optional if script_path is used)")
    script_path: Optional[str] = Field(default=None, description="Path to a .py file containing the analysis code (preferred for complex analysis)")
    filter: Optional[str] = Field(default=None, description="OData filter to apply before fetching (e.g., \"Perioden eq '2023JJ00'\")")
    select: Optional[List[str]] = Field(default=None, description="Column names to fetch (reduces data transfer)")
    top: int = Field(default=10000, ge=1, le=100000, description="Maximum records to fetch (default: 10000)")
    translate: bool = Field(default=True, description="Translate coded dimension values to human-readable text")


class AnalyzeLocalInput(BaseModel):
    """Input model for local dataset analysis."""
    model_config = ConfigDict(str_strip_whitespace=True)
    dataset_name: str = Field(..., min_length=1, description="Dataset name (e.g., 'test_dataset.csv')")
    analysis_code: Optional[str] = Field(default=None, description="Python code to execute on 'df' DataFrame (optional if script_path is used)")
    script_path: Optional[str] = Field(default=None, description="Path to a .py file containing the analysis code (preferred for complex analysis)")


class QueryDatasetInput(BaseModel):
    """Input model for querying datasets."""
    model_config = ConfigDict(str_strip_whitespace=True)
    dataset_id: str = Field(..., min_length=1, description="Dataset ID (e.g., '85313NED')")
    top: int = Field(default=10, ge=1, le=10000, description="Number of records to return")
    skip: int = Field(default=0, ge=0, description="Number of records to skip")
    filter: Optional[str] = Field(default=None, description="OData filter (e.g., \"Perioden eq '2023JJ00'\")")
    select: Optional[List[str]] = Field(default=None, description="Column names to return")
    compact: bool = Field(default=True, description="Return summary for large results")
    translate: bool = Field(default=True, description="Translate coded dimension values to human-readable text (e.g., '3000' -> 'Mannen')")


class MetadataType(str, Enum):
    """Metadata type options for unified metadata tool."""
    INFO = "info"           # TableInfos - dataset description
    STRUCTURE = "structure" # DataProperties - column definitions
    ENDPOINTS = "endpoints" # Root metadata - available endpoints
    DIMENSIONS = "dimensions"  # Dimension values with codes for filtering
    CUSTOM = "custom"       # Custom endpoint (use endpoint_name)


class GetMetadataInput(BaseModel):
    """Input model for unified metadata retrieval."""
    model_config = ConfigDict(str_strip_whitespace=True)
    dataset_id: str = Field(..., min_length=1, description="Dataset ID (e.g., '85313NED')")
    metadata_type: MetadataType = Field(default=MetadataType.INFO, description="Type of metadata: 'info', 'structure', 'endpoints', 'dimensions', or 'custom'")
    endpoint_name: Optional[str] = Field(default=None, description="Endpoint/dimension name (required for 'dimensions' and 'custom' types, e.g., 'Geslacht', 'Perioden')")
