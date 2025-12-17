"""
NL OpenData MCP Server - Access Dutch government open data from CBS and data.overheid.nl.

This package provides an MCP (Model Context Protocol) server for accessing
Dutch government statistics and open data.
"""
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("nl-opendata-mcp")
except PackageNotFoundError:
    __version__ = "0.4.8"

from .config import Settings, get_settings

__all__ = [
    "__version__",
    "Settings",
    "get_settings",
]
