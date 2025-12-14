"""
Pytest configuration and fixtures for nl-opendata-mcp tests.
"""
import pytest
import os
import sys

# Enable python analysis for tests
os.environ["NL_OPENDATA_MCP_USE_PYTHON_ANALYSIS"] = "true"

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class MockContext:
    """Mock FastMCP context for testing tools."""

    def __init__(self):
        self.info_messages = []
        self.error_messages = []
        self.warning_messages = []

    def info(self, msg: str):
        self.info_messages.append(msg)
        print(f"[INFO] {msg}")

    def error(self, msg: str):
        self.error_messages.append(msg)
        print(f"[ERROR] {msg}")

    def warning(self, msg: str):
        self.warning_messages.append(msg)
        print(f"[WARNING] {msg}")

    def clear(self):
        self.info_messages.clear()
        self.error_messages.clear()
        self.warning_messages.clear()


@pytest.fixture
def mock_context():
    """Provide a mock context for tool testing."""
    return MockContext()


@pytest.fixture
def settings():
    """Provide settings instance."""
    from nl_opendata_mcp.config import get_settings
    return get_settings()


def get_fn(tool):
    """Extract the callable function from a FunctionTool wrapper."""
    if hasattr(tool, 'fn'):
        return tool.fn
    return tool
