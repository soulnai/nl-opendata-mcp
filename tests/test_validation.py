"""
Tests for input validation and security functions.
"""
from nl_opendata_mcp import server
import pytest
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nl_opendata_mcp.utils import (
    validate_dataset_id,
    sanitize_odata_filter,
    sanitize_select_columns,
    safe_join_path,
    ValidationError,
    PathTraversalError,
)


class TestDatasetIdValidation:
    """Tests for dataset ID validation."""

    def test_valid_cbs_id(self):
        """Test valid CBS dataset ID."""
        result = validate_dataset_id("85313NED")
        assert result == "85313NED"

    def test_valid_id_with_underscores(self):
        """Test valid ID with underscores."""
        result = validate_dataset_id("test_dataset_123")
        assert result == "test_dataset_123"

    def test_valid_id_with_hyphens(self):
        """Test valid ID with hyphens."""
        result = validate_dataset_id("groningen-parkeervakken")
        assert result == "groningen-parkeervakken"

    def test_strips_whitespace(self):
        """Test that whitespace is stripped."""
        result = validate_dataset_id("  85313NED  ")
        assert result == "85313NED"

    def test_empty_id_raises(self):
        """Test that empty ID raises error."""
        with pytest.raises(ValidationError):
            validate_dataset_id("")

    def test_whitespace_only_raises(self):
        """Test that whitespace-only raises error."""
        with pytest.raises(ValidationError):
            validate_dataset_id("   ")

    def test_invalid_characters_raises(self):
        """Test that invalid characters raise error."""
        with pytest.raises(ValidationError):
            validate_dataset_id("test/path")

    def test_semicolon_raises(self):
        """Test that semicolons raise error."""
        with pytest.raises(ValidationError):
            validate_dataset_id("test;injection")


class TestODataFilterSanitization:
    """Tests for OData filter sanitization."""

    def test_none_returns_none(self):
        """Test that None input returns None."""
        assert sanitize_odata_filter(None) is None

    def test_empty_returns_none(self):
        """Test that empty string returns None."""
        assert sanitize_odata_filter("") is None

    def test_valid_eq_filter(self):
        """Test valid eq filter."""
        result = sanitize_odata_filter("Perioden eq '2023JJ00'")
        assert result == "Perioden eq '2023JJ00'"

    def test_valid_substringof(self):
        """Test valid substringof filter."""
        result = sanitize_odata_filter("substringof('Amsterdam', RegioS)")
        assert "substringof" in result

    def test_valid_and_or(self):
        """Test valid and/or filter."""
        result = sanitize_odata_filter("Field1 eq 'a' and Field2 eq 'b'")
        assert result is not None

    def test_dangerous_characters_raise(self):
        """Test that dangerous characters raise error."""
        with pytest.raises(ValidationError):
            sanitize_odata_filter("Field eq 'value'; DROP TABLE users")

    def test_unbalanced_parens_raise(self):
        """Test that unbalanced parentheses raise error."""
        with pytest.raises(ValidationError):
            sanitize_odata_filter("substringof('test', Field")

    def test_unbalanced_quotes_raise(self):
        """Test that unbalanced quotes raise error."""
        with pytest.raises(ValidationError):
            sanitize_odata_filter("Field eq 'unclosed")

    def test_too_long_filter_raises(self):
        """Test that very long filter raises error."""
        long_filter = "Field eq '" + "x" * 3000 + "'"
        with pytest.raises(ValidationError):
            sanitize_odata_filter(long_filter)


class TestColumnNameSanitization:
    """Tests for column name sanitization."""

    def test_valid_column_name(self):
        """Test valid column name."""
        result = sanitize_select_columns(["Column1", "Column2"])
        assert result == ["Column1", "Column2"]

    def test_none_returns_none(self):
        """Test that None returns None."""
        assert sanitize_select_columns(None) is None

    def test_strips_whitespace(self):
        """Test that whitespace is stripped."""
        result = sanitize_select_columns(["  Column1  "])
        assert result == ["Column1"]

    def test_underscore_allowed(self):
        """Test that underscores are allowed."""
        result = sanitize_select_columns(["my_column"])
        assert result == ["my_column"]

    def test_empty_name_raises(self):
        """Test that empty column name raises error."""
        with pytest.raises(ValidationError):
            sanitize_select_columns([""])

    def test_invalid_characters_raise(self):
        """Test that invalid characters raise error."""
        with pytest.raises(ValidationError):
            sanitize_select_columns(["column; DROP TABLE"])


class TestPathTraversalProtection:
    """Tests for path traversal protection."""

    def test_simple_filename(self):
        """Test simple filename."""
        result = safe_join_path("/base/dir", "file.csv")
        assert result.endswith("file.csv")
        assert "/base/dir" in result or "\\base\\dir" in result

    def test_path_traversal_blocked(self):
        """Test that path traversal is blocked."""
        # Should strip path components
        result = safe_join_path("/base/dir", "../../../etc/passwd")
        assert "passwd" in result
        assert result.startswith("/base/dir") or "base" in result

    def test_absolute_path_blocked(self):
        """Test that absolute paths are blocked."""
        result = safe_join_path("/base/dir", "/etc/passwd")
        assert "passwd" in result
        # Should only keep basename
        assert not result.startswith("/etc")

    def test_empty_filename_raises(self):
        """Test that empty filename raises error."""
        with pytest.raises(ValidationError):
            safe_join_path("/base/dir", "")

    def test_dotfile_blocked(self):
        """Test that dotfiles are blocked."""
        with pytest.raises(ValidationError):
            safe_join_path("/base/dir", ".hidden")

    def test_null_byte_blocked(self):
        """Test that null bytes are blocked."""
        with pytest.raises(ValidationError):
            safe_join_path("/base/dir", "file\x00.csv")
